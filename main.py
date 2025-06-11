from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import requests
import json
import pyproj
from pyproj import Transformer
import calendar

app = Flask(__name__)


@app.route('/dataconverter', methods=['POST'])


def endpoint():
    try:
        wdme_msg = request.get_json()

        if 'datasource' in wdme_msg or 'entityId' in wdme_msg or 'resourceid' in wdme_msg or 'id' in wdme_msg:

            if 'datasource' in wdme_msg: 
                datasource = wdme_msg['datasource']
            elif 'entityId' in wdme_msg:
                datasource = wdme_msg['entityId']
            elif 'resourceid' in wdme_msg:
                datasource = wdme_msg['resourceid']
            elif 'id' in wdme_msg:
                datasource  = wdme_msg['id'] 

            
            if datasource == "KNMI":
                converted_data = convert_data_pwn_knmi(wdme_msg["data"])
                response = {
                    'message': 'Data from KNMI received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "KNMI-2":
                converted_data = convert_data_pwn_knmi_2(wdme_msg["data"])
                response = {
                    'message': 'Data from KNMI received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "RWS":
                converted_data = convert_data_pwn_rws(wdme_msg["data"])
                response = {
                    'message': 'Data from RWS received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "RWS-2":
                converted_data = convert_data_pwn_rws_2(wdme_msg["data"])
                response = {
                    'message': 'Data from RWS received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "PWN-PREDICTION":
                converted_data = convert_data_pwn_prediction(wdme_msg["data"])
                response = {
                    'message': 'Data from PWN received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "bf191342-dfa4-42c6-aaf3-0986d3cf7c4a": # HWL resource id
                converted_data = convert_data_pwn_hwl(wdme_msg["data"])
                response = {
                    'message': 'Data from HWL received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            elif datasource == "ferryboatdata": # IoT ferryboat sensor resource id
                converted_data = convert_data_iot_sensor(wdme_msg["data"])
                response = {
                    'message': 'Data from IoT ferryboat sensor received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            elif datasource == "84e5b237-4244-48fb-8376-0943271c0f90": # Riwa Rijn resource id
                converted_data = convert_data_riwa_rijn(wdme_msg["data"])
                response = {
                    'message': 'Data from Riwa Rijn received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            elif datasource == "cac83e99-5565-4f51-8331-593be17cabd6": # PA resource id
                first_sensor_name = wdme_msg['data'][0].get('NAME', '')
                
                water_sensors = {"ANWYAA_QT10", "ANWYAA_QT20", "AWWYIB_QT40"}
                wind_sensors = {"AWA0XT_WT10", "AWA0XT_ST10", "AWA0XT_GT10"}

                if first_sensor_name in water_sensors:
                    converted_data = convert_data_pa_water_sensors(wdme_msg["data"])
                    response_message = 'Data from PA water sensors received and converted successfully'
                elif first_sensor_name in wind_sensors:
                    converted_data = convert_data_pa_wind_sensors(wdme_msg["data"])
                    response_message = 'Data from PA wind sensors received and converted successfully'
                else:
                    return jsonify({'error': f'Unknown sensor name: {first_sensor_name}'}), 400
                
                response = {
                    'message': response_message,
                    'converted_data': converted_data
                }
                return jsonify(response)
            elif datasource == "EnvironmentDataAgency":
                converted_data = convert_data_sww_envdata(wdme_msg["data"])
                response = {
                    'message': 'Data from SWW Environment Data Agency received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "MeteorDataCloud-WaterQuality":
                converted_data = convert_data_sww_wq(wdme_msg["data"])
                response = {
                    'message': 'Data from SWW MeteorDataCloud-WaterQuality received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response) 
            
            elif datasource == "SWW-SFTP-ANALOG":
                converted_data = convert_data_sww_analog(wdme_msg["data"])
                response = {
                    'message': 'Data from SWW SFTP analog received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "SWW-SFTP-DIGITAL":
                converted_data = convert_data_sww_digital(wdme_msg["data"])
                response = {
                    'message': 'Data from SWW SFTP digital received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "hst-opcua":
                converted_data = convert_data_hst_rainfall_forecast(wdme_msg["data"])
                response = {
                    'message': 'Data from HST-OPCUA received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif (datasource == "urn:ngsi-ld:WaterObserved:eui-24e124126c144958" or
                   datasource == "urn:ngsi-ld:WaterObserved:eui-24e124126c145086"):
                converted_data = convert_data_hst_waterObserved(wdme_msg)
                response = {
                    'message': 'Data from Etteln QI received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif (datasource == "urn:ngsi-ld:WeatherObserved:eui-0600005e204f523c" or 
                   datasource == "urn:ngsi-ld:WeatherObserved:eui-060000702f83bf8c" or
                   datasource == "urn:ngsi-ld:WeatherObserved:eui-06000078f13c9e40" or
                   datasource == "urn:ngsi-ld:WeatherObserved:eui-0600007ab44894e8" or
                   datasource == "urn:ngsi-ld:WeatherObserved:eui-0600007b59123343"):
                converted_data = convert_data_hst_weatherObserved(wdme_msg)
                response = {
                    'message': 'Data from Etteln QI received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif (datasource == "urn:ngsi-ld:FloodMonitoring:eui-a840411f7188c97e" or 
                   datasource == "urn:ngsi-ld:FloodMonitoring:eui-a84041208188c97f" or
                   datasource == "urn:ngsi-ld:FloodMonitoring:eui-a840414c0188c972" or
                   datasource == "urn:ngsi-ld:FloodMonitoring:eui-a840415e9188c976" or
                   datasource == "urn:ngsi-ld:FloodMonitoring:eui-a84041df11851789" or
                   datasource == "urn:ngsi-ld:FloodMonitoring:eui-a84041e5f188c97b"):
                converted_data = convert_data_hst_floodMonitoring(wdme_msg)
                response = {
                    'message': 'Data from Etteln QI received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "WBL-DATASOURCE":
                converted_data = convert_data_wbl_call_complains(wdme_msg["data"])
                response = {
                    'message': 'Data from WBL Call Complaints received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "WBL-DATASOURCE-TELEMETRY":
                converted_data = convert_data_wbl_telemetry(wdme_msg["data"])
                response = {
                    'message': 'Data from WBL Telemetry received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "WBL-DATASOURCE-SMARTMETERING":
                converted_data = convert_data_wbl_smart_metering(wdme_msg["data"])
                response = {
                    'message': 'Data from WBL Smart Metering received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "FY-WATER-DUCT":
                converted_data = convert_data_key_water_duct(wdme_msg["data"])
                response = {
                    'message': 'Data from Keyaqua water duct received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "FY-WATER-NODE":
                converted_data = convert_data_key_water_node(wdme_msg["data"])
                response = {
                    'message': 'Data from Keyaqua water node received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "FY-KEYAQUA-DUCT-CONDITIONS":
                converted_data = convert_data_key_duct_conditions(wdme_msg["data"])
                response = {
                    'message': 'Data from Keyaqua duct conditions received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "FY-SEWER-PUMPING-STATION":
                converted_data = convert_data_key_sewer_pumping_station(wdme_msg["data"])
                response = {
                    'message': 'Data from Keyaqua sewer pumping station received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "FY-SEWER-VALVE":
                converted_data = convert_data_key_sewer_valve(wdme_msg["data"])
                response = {
                    'message': 'Data from Keyaqua sewer valve received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "FY-FLOW-METERS":
                converted_data = convert_data_key_flow_meters(wdme_msg["data"])
                response = {
                    'message': 'Data from Keyaqua flow meters received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "FY-FLOW-METERS-READING":
                converted_data = convert_data_key_flow_meter_reading(wdme_msg["data"])
                response = {
                    'message': 'Data from Keyaqua flow meter reading received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "FY-SEWER-MANHOLE":
                converted_data = convert_data_key_sewer_manhole(wdme_msg["data"])
                response = {
                    'message': 'Data from Keyaqua sewer manhole received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            elif datasource == "2065410-6043-4a9c-90cb-123456b8b3de" or datasource == "20138902-6043-4a9c-90cb-123456b8b3de":
                converted_data = convert_data_key_t3_t11(wdme_msg["data"])
                response = {
                    'message': 'Data from Keyaqua T3-T11 received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            elif datasource == "20347104-6043-4a9c-90cb-123456b8b3de":
                converted_data = convert_data_key_usecase_3(wdme_msg["data"])
                response = {
                    'message': 'Data from Keyaqua use case 3 received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            elif datasource == "20346905-6043-4a9c-90cb-123456b8b3de":
                converted_data = convert_data_key_cgi(wdme_msg["data"])
                response = {
                    'message': 'Data from Keyaqua cgi received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            elif datasource == "SP-SNOWFLAKE-SQL-1":
                converted_data = convert_data_hidr_snowflake_sql_1(wdme_msg["data"])
                response = {
                    'message': 'Data from Hidralia snowflake SQL-1 received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "SP-SNOWFLAKE-SQL-2":
                converted_data = convert_data_hidr_snowflake_sql_2(wdme_msg["data"])
                response = {
                    'message': 'Data from Hidralia snowflake SQL-2 received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "SP-AGBAR-HYDRAULIC-EFFICENCY":
                converted_data = convert_data_hidr_hydraulic_efficiency(wdme_msg["data"])
                response = {
                    'message': 'Data from Hidralia hydraulic efficiency received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "SP-AGBAR-LEAKS":
                converted_data = convert_data_hidr_leaks(wdme_msg["data"])
                response = {
                    'message': 'Data from Hidralia leaks received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "SP-AGBAR-ENERGY":
                converted_data = convert_data_hidr_energy(wdme_msg["data"])
                response = {
                    'message': 'Data from Hidralia energy received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "SP-AGBAR-WATER-SOURCES-D":
                converted_data = convert_data_hidr_water_sources_D(wdme_msg["data"])
                response = {
                    'message': 'Data from Hidralia water sources D received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)
            
            elif datasource == "SP-AGBAR-WATER-SOURCES-M":
                converted_data = convert_data_hidr_water_sources_M(wdme_msg["data"])
                response = {
                    'message': 'Data from Hidralia water sources M received and converted successfully',
                    'converted_data': converted_data
                }
                return jsonify(response)

            else:
                return jsonify({"error": "Invalid data format or missing keys."}), 400
            
        else:
            error_message = {'error': 'Missing datasource in received data'}
            return jsonify(error_message), 400 #bad request status, or 422 (?)

    except Exception as e:
            error_message = {'error': str(e)}
            return jsonify(error_message), 500 #internal server error

#############################################################################################################

def convert_time(input_time_string):
            try:
                time = datetime.strptime(input_time_string, "%Y-%m-%d %H:%M:%S")
                date = time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00Z"
                return date
            except ValueError:
                try:
                    time = datetime.strptime(input_time_string, "%Y-%m-%dT%H:%M:%S")
                    date = time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00Z"
                    return date
                except ValueError:
                    input_time_string = input_time_string [:-3]
                    try: 
                        time = datetime.strptime(input_time_string, "%Y-%m-%d %H:%M:%S.%f")
                        date = time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00Z"
                        return date
                    except ValueError:
                        try:
                            time = datetime.strptime(input_time_string, "%Y-%m-%dT%H:%M:%S.%f")
                            date = time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00Z"
                            return date
                        except ValueError as e:
                            return{"error": str(e)}
                        

def string_to_float (string_number):

    if isinstance(string_number, str) and ',' in string_number:
        string_number = string_number.replace(',', '.')

    try:
        float_number = float(string_number)
        return float_number
    except ValueError:
        return None

# NL - KNMI data ##############################################################################################

def convert_data_pwn_knmi(data):

    # check data fields

    data_list = []

    for obj in data:

        station_code = obj['station_code']
        date = obj['date']
        hour = obj['hour']
        minute = 0
        windDirection = obj['DD']
        windSpeed = obj['FH']
        hourlyPrecipitation = obj['RH']

        if hour == 24: 
            hour = 23
            minute = 59


        dt = datetime.fromisoformat(date.replace('Z', '+00:00'))
        dt = dt.replace(hour=hour, minute=minute)
        date = dt.isoformat(timespec='milliseconds') + 'Z'

        # locations_endpoint = 'https://dataportal-waterverse.opsi.lecce.it/dataset/6622fa01-0f71-470f-9790-3557cfe21e8b/resource/8310893f-52b9-445d-bdd1-2e2610780f9b/download/final_location_knmi.json'
        # r = requests.get(locations_endpoint)
        # locations_data = r.json()

        with open("knmi_locations.json", "r") as json_file:
            locations_data = json.load(json_file)

        station_info = locations_data[str(station_code)]
        stationName = station_info['stationName']
        coordinates = station_info['location']['coordinates']


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:WeatherObserved:NL-KNMI-" +str(station_code),
            "type": "WeatherObserved",
            "dateObserved": date,
            "location": {
                "type": "Point",
                "coordinates": coordinates
                },
            "dataProvider": "KNMI",
            "windDirection": windDirection,
            "windSpeed": windSpeed,
            "precipitation": hourlyPrecipitation,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Weather/master/context.jsonld"
  ]
        }

        data_list.append(converted_data)


    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# NL - KNMI-2 data ################################################################################

def convert_data_pwn_knmi_2(data):

    # check data fields

    data_list = []

    for obj in data:

        station_code = obj['station_code']
        date = obj['date']
        minute = 0
        hour = 0
        windDirection = obj['DDVEC']
        windSpeed = obj['FHVEC']
        temperature = obj['TG']
        precipitation = obj['RH']
        evaporation = obj['EV24']

        if hour == 24: 
            hour = 23
            minute = 59


        dt = datetime.fromisoformat(date.replace('Z', '+00:00'))
        dt = dt.replace(hour=hour, minute=minute)
        date = dt.isoformat(timespec='milliseconds') + 'Z'

        with open("knmi_locations.json", "r") as json_file:
            locations_data = json.load(json_file)

        station_info = locations_data[str(station_code)]
        stationName = station_info['stationName']
        coordinates = station_info['location']['coordinates']


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:WeatherObserved:NL-KNMI-2-" +str(station_code),
            "type": "WeatherObserved",
            "dateObserved": date,
            "location": {
                "type": "Point",
                "coordinates": coordinates
                },
            "dataProvider": "KNMI",
            "windDirection": windDirection,
            "windSpeed": windSpeed,
            "precipitation": precipitation,
            "temperature": temperature,
            "relativeHumidity": evaporation,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Weather/master/context.jsonld"
  ]
        }

        data_list.append(converted_data)


    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# NL - RWS data ###################################################################################

def convert_data_pwn_rws(data):

    # check data fields,

    data_list = []

    for obj in data:

        location_code = obj['locationCode']
        station_name = obj['stationName']
        station_name = station_name.replace(' ', '')
        date = obj['dateObserved']
        measured_value = obj['measureValue']

        source_crs = pyproj.CRS("EPSG:25831")
        target_crs = pyproj.CRS("EPSG:4326")
        transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)
        x, y = obj['X'], obj['Y']
        lon, lat = transformer.transform(x, y)
        coordinates = [lon, lat]
        
        date = date.replace('.00Z', '.000Z')
        dt = datetime.fromisoformat(date.replace('Z', '+00:00'))
        date = dt.isoformat(timespec='milliseconds') + 'Z'

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:WaterQualityObserved:NL-RWS-"+str(station_name)+"-"+str(location_code),
            "type": "WaterQualityObserved",
            "dateObserved": date,
            "location": {
                "type": "Point",
                "coordinates": coordinates
                },
            "dataProvider": "RWS",
            "Cl-": measured_value,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.WaterQuality/master/context.jsonld"
  ]
        }

        data_list.append(converted_data)


    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# NL - RWS-2 data ###################################################################################################

def convert_data_pwn_rws_2(data):

    data_list = []

    for obj in data:

        value = obj['value']
        parameter = obj['parameter']
        description = obj['description']
        location_code = obj['location_code']
        location = obj['location']
        date = obj['timestamp']
        formated_date = date + ".000+00:00Z"
        
        if description == "Debiet Oppervlaktewater m3/s":
            controledProperty = "Surface water flow"
            propertyUnit = "m3/s"
        else:
            controledProperty = "Concentration of chloride in surface water"
            propertyUnit = "mg/l"


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:DeviceMeasurement:NL-RWS-2-"+str(parameter)+"-"+str(location)+"-"+str(location_code),
            "type": "DeviceMeasurement",
            "dateObserved": formated_date,
            "description": description,
            "dataProvider": "RWS",
            "numValue": value,
            "deviceType": "sensor",
            "controlledProperty": controledProperty,
            "unit": propertyUnit,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld"
  ]
        }

        source_crs = pyproj.CRS("EPSG:25831")
        target_crs = pyproj.CRS("EPSG:4326")
        transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)
        x, y = obj['X'], obj['Y']

        if x and y:
            lon, lat = transformer.transform(x, y)
            coordinates = [lon, lat]
            
            converted_data["location"] = {
                "type": "Point",
                "coordinates": coordinates
            }

        data_list.append(converted_data)


    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# NL - CL Prediction data ###########################################################################################

def convert_data_pwn_prediction(msg):

    data_list = []

    coordinates = msg["metadata"]["location"]["coordinates"]
    entityId = msg["metadata"]["entityId"]
    entity_id = entityId.split(":")[-1]

    for obj in msg['data']:

        datePredicted = obj['date']
        valuePredicted = obj['value']

        date = datetime.fromisoformat(datePredicted)
        datePredicted = date.strftime("%Y-%m-%dT%H:%M:%S.000+00:00Z")

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:WaterQualityPredicted:"+entity_id,
            "type": "WaterQualityPredicted",
            "dataProvider": "PWN",
            "areaServed": "Andijk",
            "location": {
                "type": "Point",
                "coordinates": coordinates
                },
            "description": "Predicted chloride concentrations for the next 7 days in Andijk",
            "datePredicted": datePredicted,
            "prediction": valuePredicted,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.WaterQuality/master/context.jsonld"
  ]
        }

        data_list.append(converted_data)


    if 'invalid' in msg:
        raise ValueError('Invalid data')

    return data_list


# NL - HWL data ###################################################################################################

def convert_data_pwn_hwl(data):
    data_list = []
    for obj in data:
        try:
            # Extract fields from the record
            id = obj.get('id', 'unknown_id')
            analysis_date = obj.get('Analysedatum', 'unknown_date')
            sample_date = obj.get('Monsterdatum', 'unknown_date')
            concentration = obj.get('Waarde', None)
            analysis_type = obj.get('Analyse', 'unknown_analysis')
            component = obj.get('Component', 'unknown_component')
            result = obj.get('Resultaat', 'unknown_result')
            place = obj.get("Plaats", 'unknown_place')
            sub_location = obj.get('Sublocatie', 'unknown_sublocatie')
            sampling_point = obj.get('Monsterpunt', 'unknown_monsterpunt')
            unit = obj.get('Eenheid', 'unknown_unit')

            # Format dates safely
            formatted_sample_date = f"{sample_date}.000+00:00Z"
            formatted_analysis_date = f"{analysis_date}.000+00:00Z"

            # Check for missing critical fields and log details
            required_fields = {
                'id': id,
                'Analysedatum': analysis_date,
                'Monsterdatum': sample_date,
                'Waarde': concentration,
                'Analyse': analysis_type,
                'Component': component,
                'Resultaat': result,
                'Plaats': place,
                'Sublocatie': sub_location,
                'Monsterpunt': sampling_point,
                'Eenheid': unit
            }

            missing_fields = [field for field, value in required_fields.items() if value is None or value == "" or "unknown" in str(value)]
            if missing_fields:
                app.logger.warning(f"Skipping record due to missing fields: {missing_fields}. Full record: {obj}")
                continue  # Skip this record and move to the next one            

            # Convert data to NGSI-LD format
            converted_data = {
                "id": f"urn:ngsi-ld:WaterQualityObserved: NL-HWL: {place}-{sub_location}-{sampling_point}",
                "type": "WaterQualityObserved",
                "dateObserved": formatted_sample_date,
                "location": {
                    "type": "Point",
                    "coordinates": [5.264806, 52.750194]
                },
                "dataProvider": "PWN",
                "description": f"Result: {str(result)} (ID: {str(id)}, Analyzed on {str(formatted_analysis_date)}. Units: {str(unit)}).",
                "componentAnalyzed ": component,
                "componentName ": analysis_type,
                "concentration": concentration,
                "@context": [
                    "https://raw.githubusercontent.com/smart-data-models/dataModel.WaterQuality/master/context.jsonld"
                ]
            }
            data_list.append(converted_data)

        except Exception as e:
            # Log the error and include the problematic record
            app.logger.error(f"Error processing record: {obj}. Error: {str(e)}")
            continue  # Continue processing the next record

    return data_list

# NL - IOT sensor ferryboat data ###################################################################################################

def convert_data_iot_sensor(data_list):
    converted_data_list = []  # Store converted documents

    for data in data_list:  # Loop through each dictionary
        try:
            # Extract fields from the record with default values
            lat = data.get("position", {}).get("context", {}).get("lat", "unknown_lat")
            long = data.get("position", {}).get("context", {}).get("lng", "unknown_long")
            coordinates = [long, lat]  # Longitude first for correct geo format

            cond_value = data.get("conductivity", "unknown_conductivity")
            course = data.get("course", "unknown_course")
            speed = data.get("speed", "unknown_speed")
            datetime_str = data.get("datetime", "unknown_datetime")  # New field

            # Check for missing critical fields and log details
            required_fields = {
                'position': data.get("position"),
                'conductivity': cond_value,
                'course': course,
                'speed': speed,
                'datetime': datetime_str  # Add datetime to required fields
            }

            missing_fields = [field for field, value in required_fields.items() if not value or "unknown" in str(value)]
            if missing_fields:
                app.logger.warning(f"Skipping record due to missing fields: {missing_fields}. Full record: {data}")
                continue  # Skip this record and move to the next one

            # Ensure latitude and longitude are valid floats
            try:
                coordinates = [float(long), float(lat)]
            except ValueError:
                app.logger.error(f"Invalid coordinates: {coordinates}. Full record: {data}")
                continue  # Skip this record if coordinates are invalid

            # Harmonize the datetime field
            try:
                # Parse the datetime string and reformat it
                datetime_obj = datetime.fromisoformat(datetime_str)
                formatted_datetime = datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            except ValueError:
                app.logger.error(f"Invalid datetime format: {datetime_str}. Full record: {data}")
                continue  # Skip this record if datetime is invalid

            # Convert data to NGSI-LD format
            converted_data = {
                "id": f"urn:ngsi-ld:WaterQualityObserved:NL-PWN-ferryboat-sensor-{course}-{speed}",
                "type": "WaterQualityObserved",
                "dateCreated": formatted_datetime,  
                "location": {
                    "type": "Point",
                    "coordinates": coordinates
                },
                "dataProvider": "PWN",
                "conductivity": cond_value,
                "description": f"Ferryboat course: {str(course)} deg, speed: {str(speed)} knots.",
                "@context": [
                    "https://raw.githubusercontent.com/smart-data-models/dataModel.WaterQuality/master/context.jsonld"
                ]
            }

            converted_data_list.append(converted_data)  # Append to result list

        except Exception as e:
            # Log the error and skip the problematic record
            app.logger.error(f"Error processing record: {data}. Error: {str(e)}")
            continue  # Continue processing the next record

    return converted_data_list  # Return list of converted documents


# NL - RIWA-Rijn data ###################################################################################################

def convert_data_riwa_rijn(data):
    data_list = []

    for obj in data:
        try:
            # Extract fields from the record with default values
            x = obj.get("x-coördinaat", "unknown_x")
            y = obj.get("y-coördinaat", "unknown_y")
            location_code = obj.get("rp", "unknown_rp")
            location_name = obj.get("omschrijving", "unknown_location")
            parameter = obj.get("par", "unknown_parameter")
            NL_name = obj.get("naam", "unknown_name")
            ENG_name = obj.get("Engelse naam", "unknown_eng_name")
            cas_number = obj.get("cas-nummer", "unknown_cas")
            date = obj.get("datum", "unknown_date")
            limit = obj.get("teken", "unknown_limit")
            value = obj.get("waarde", "unknown_value")
            unit = obj.get("dimensie", "unknown_unit")

            # Check for missing critical fields and log details
            required_fields = {
                'x-coördinaat': x,
                'y-coördinaat': y,
                'rp': location_code,
                'naam': NL_name,
                'Engelse naam': ENG_name,
                'cas-number': cas_number,
                'datum': date,
                'waarde': value,
                'dimensie': unit
            }

            missing_fields = [field for field, value in required_fields.items() if not value or "unknown" in str(value)]
            if missing_fields:
                app.logger.warning(f"Skipping record due to missing fields: {missing_fields}. Full record: {obj}")
                continue  # Skip this record and move to the next one

            # Format date safely
            formatted_date = f"{date}.000+00:00Z"

            # Determine the description based on the limit field
            if limit == "+":
                description = "(+) Measurement was above the reporting limit."
            elif limit == "<":
                description = "(<) Measurement was below the reporting limit."
            else:
                description = "Measurement limit information not available."

            description += f" Units: {unit}. CAS: {cas_number}, Parameter: {parameter}"

            # Convert data to NGSI-LD format
            converted_data = {
                "id": f"urn:ngsi-ld:WaterQualityObserved:NL-PWN-RIWA-Rijn-{location_code}-{location_name}",
                "type": "WaterQualityObserved",
                "dateObserved": formatted_date,
                "dataProvider": "RIWA Rijn",
                "componentName": NL_name,  # Need to be added in the SDM, attribute name may change
                "measurand": [ENG_name],  
                "concentration": value,  # Need to be added in the SDM, attribute name may change
                "description": description,
                "@context": [
                    "https://raw.githubusercontent.com/smart-data-models/dataModel.WaterQuality/master/context.jsonld"
                ]
            }

            # Transform coordinates if x and y are valid
            if x != "unknown_x" and y != "unknown_y":
                try:
                    source_crs = pyproj.CRS("EPSG:25831")
                    target_crs = pyproj.CRS("EPSG:4326")
                    transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)
                    lon, lat = transformer.transform(float(x), float(y))
                    coordinates = [lon, lat]

                    converted_data["location"] = {
                        "type": "Point",
                        "coordinates": coordinates
                    }
                except (ValueError, pyproj.exceptions.ProjError) as e:
                    app.logger.error(f"Invalid coordinates: x={x}, y={y}. Full record: {obj}. Error: {str(e)}")
                    continue  # Skip this record if coordinates are invalid

            # Append the converted data to the list
            data_list.append(converted_data)

        except Exception as e:
            # Log the error and skip the problematic record
            app.logger.error(f"Error processing record: {obj}. Error: {str(e)}")
            continue  # Continue processing the next record

    return data_list

# NL - PA data from water sensors ###################################################################################################

def convert_data_pa_water_sensors(data):
    data_list = []

    for obj in data:
        try:
            # Extract fields from the record with default values
            sensor_name = obj.get('NAME', 'unknown_sensor')
            measured_value = obj.get('IP_TREND_VALUE', 'unknown_value')
            date = obj.get('IP_TREND_TIME', 'unknown_date')
            quality_result = obj.get('IP_INPUT_QUALITY', 'unknown_quality')
            description = obj.get('IP_DESCRIPTION', 'unknown_description')
            unit = obj.get('IP_ENG_UNITS', 'unknown_unit')

            # Check for missing critical fields and log details
            required_fields = {
                'NAME': sensor_name,
                'IP_TREND_VALUE': measured_value,
                'IP_TREND_TIME': date,
                'IP_INPUT_QUALITY': quality_result,
                'IP_DESCRIPTION': description,
                'IP_ENG_UNITS': unit
            }

            missing_fields = [field for field, value in required_fields.items() if not value or "unknown" in str(value)]
            if missing_fields:
                app.logger.warning(f"Skipping record due to missing fields: {missing_fields}. Full record: {obj}")
                continue  # Skip this record and move to the next one

            # Format timestamp safely
            formatted_timestamp = date + "Z"

            # Define default coordinates
            coordinates = [5.264806, 52.750194]

            # Create description string
            description_text = f"Result: {quality_result}. Measurement from sensor: {sensor_name}, {description}. Units: {unit}."

            # Convert data to NGSI-LD format
            converted_data = {
                "id": f"urn:ngsi-ld:WaterQualityObserved:NL-PWN-PA-Water-Sensors-{sensor_name}",
                "type": "WaterQualityObserved",
                "dateObserved": formatted_timestamp,
                "location": {
                    "type": "Point",
                    "coordinates": coordinates
                },
                "dataProvider": "PWN",
                "conductivity": measured_value,
                "description": description_text,
                "@context": [
                    "https://raw.githubusercontent.com/smart-data-models/dataModel.WaterQuality/master/context.jsonld"
                ]
            }

            data_list.append(converted_data)

        except Exception as e:
            # Log the error and skip the problematic record
            app.logger.error(f"Error processing record: {obj}. Error: {str(e)}")
            continue  # Continue processing the next record

    return data_list

# NL - PA data from wind sensors ###################################################################################################

def convert_data_pa_wind_sensors(data):
    data_list = []

    for obj in data:
        try:
            # Extract fields from the record with default values
            sensor_name = obj.get('NAME', 'unknown_sensor')
            measured_value = obj.get('IP_TREND_VALUE', 'unknown_value')
            date = obj.get('IP_TREND_TIME', 'unknown_date')
            quality_result = obj.get('IP_INPUT_QUALITY', 'unknown_quality')
            description = obj.get('IP_DESCRIPTION', 'unknown_description')
            unit = obj.get('IP_ENG_UNITS', 'unknown_unit')

            # Check for missing critical fields and log details
            required_fields = {
                'NAME': sensor_name,
                'IP_TREND_VALUE': measured_value,
                'IP_TREND_TIME': date,
                'IP_INPUT_QUALITY': quality_result,
                'IP_DESCRIPTION': description,
                'IP_ENG_UNITS': unit
            }

            missing_fields = [field for field, value in required_fields.items() if not value or "unknown" in str(value)]
            if missing_fields:
                app.logger.warning(f"Skipping record due to missing fields: {missing_fields}. Full record: {obj}")
                continue  # Skip this record and move to the next one

            # Format timestamp safely
            formatted_timestamp = date + "Z"

            # Define default coordinates
            coordinates = [5.264806, 52.750194]

            # Create description string
            description_text = f"Result: {quality_result}. Measurement from sensor: {sensor_name}, {description}. Units: {unit}."

            # Convert data to NGSI-LD format
            converted_data = {
                "id": f"urn:ngsi-ld:WeatherObserved:NL-PWN-PA-Wind-Sensors",
                "type": "WeatherObserved",
                "dateObserved": formatted_timestamp,
                "location": {
                    "type": "Point",
                    "coordinates": coordinates
                },
                "dataProvider": "PWN",
                "description": description_text,
                "@context": [
                    "https://raw.githubusercontent.com/smart-data-models/dataModel.Weather/master/context.jsonld"
                ]
            }

            # Determine the correct attribute based on description and append it
            attribute_mapping = {
                "Windstoten": "gustSpeed",
                "Windsnelheid": "windSpeed",
                "Windrichting": "windDirection"
            }

            attribute_key = attribute_mapping.get(description)
            if attribute_key:
                converted_data[attribute_key] = measured_value

            data_list.append(converted_data)

        except Exception as e:
            # Log the error and skip the problematic record
            app.logger.error(f"Error processing record: {obj}. Error: {str(e)}")
            continue  # Continue processing the next record

    return data_list

# UK - SWW - env-data ##############################################################################################

def convert_data_sww_envdata(data):

    data_list = []

    for entry in data:

        station_code = entry["stationCode"]
        if station_code == "46155":
            coordinates = [50.428979, -3.681414]
        elif station_code == "46122":
            coordinates = [50.479033, -3.761402]
        elif station_code == "46103":
            coordinates = [50.47913, -3.760983]
        elif station_code == "46100":
            coordinates = [50.390977, -3.70144]
        elif station_code == "46101":
            coordinates = [50.393238, -3.731066]
        elif station_code == "E82080":
            coordinates = [50.391314, -3.739437]
        elif station_code == "46161":
            coordinates = [50.384444, -3.523968]
        elif station_code == "E82960":
            coordinates = [50.475407, -3.581856]
        else: coordinates = [50.479033, -3.761402]

        ############ swapping coordinates to fix incident with WDME ##############
        coordinates[0], coordinates[1] = coordinates[1], coordinates[0]

        for parameter_entry in entry["parameters"]:
            if parameter_entry["parameter"] == "level":
                parameter_name = "waterLevel"
                for measure_entry in parameter_entry["measures"]:
                    qualifier = measure_entry.get("qualifier", None)
                    label = measure_entry.get("label", None)
                    for reading in measure_entry["readings"]:
                        value = reading["value"]
                        date_time = reading["dateTime"]
                        date = datetime.fromisoformat(date_time[:-1])
                        date = date.strftime("%Y-%m-%dT%H:%M:%S")
                        date += ".000+00:00Z"

                        converted_data = {
                            "id": "urn:ngsi-ld:DeviceMeasurement:UK-SWW-env-data-agency-stationCode-"+str(station_code),
                            "type": "DeviceMeasurement",
                            "deviceType": "sensor",
                            "controlledProperty": parameter_name,
                            "unit": "meter",
                            "numValue": value,
                            "dateObserved": date,
                            "description": label,
                            "location": {
                                "type": "Point",
                                "coordinates": coordinates
                                },
                            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld"
  ]
                        }

                        data_list.append(converted_data)

            elif parameter_entry["parameter"] == "rainfall":
                parameter_name = "rainfall"
                for measure_entry in parameter_entry["measures"]:
                    qualifier = measure_entry.get("qualifier", None)
                    label = measure_entry.get("label", None)
                    for reading in measure_entry["readings"]:
                        value = reading["value"]
                        date_time = reading["dateTime"]
                        date = datetime.fromisoformat(date_time[:-1])
                        date = date.strftime("%Y-%m-%dT%H:%M:%S")
                        date += ".000+00:00Z"

                        converted_data = {
                            "id": "urn:ngsi-ld:DeviceMeasurement:UK-SWW-env-data-agency-stationCode-"+str(station_code),
                            "type": "DeviceMeasurement",
                            "deviceType": "sensor",
                            "controlledProperty": parameter_name,
                            "unit": "millimeter",
                            "numValue": value,
                            "dateObserved": date,
                            "description": label,
                            "location": {
                                "type": "Point",
                                "coordinates": coordinates
                                },
                            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld"
  ]
                        }

                        data_list.append(converted_data)




    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# UK - SWW - wq data ###############################################################################################

def convert_data_sww_wq(data):

    data_list = []

    # Create a dictionary to group readings by date and time
    grouped_readings = {}

    # Iterate through sensor readings
    for sensor_reading in data["meta"]["params"]["sensorReadings"]:
        sensor_name = sensor_reading["sensor"]
        
        # Iterate through readings for the sensor
        for reading in sensor_reading["readings"]:
            date_time = reading["dateTime"]
            value = reading["value"]
            
            # If the date and time key doesn't exist, create it
            if date_time not in grouped_readings:
                grouped_readings[date_time] = {"dateTime": date_time}
            
            # Add the sensor reading to the corresponding date and time
            grouped_readings[date_time][sensor_name] = value

    # Convert the grouped readings into a list
    grouped_readings_list = list(grouped_readings.values())

    site_id = data["meta"]["params"]["siteId"]
    site_name = data["meta"]["params"]["siteName"]
    coords_str = data["meta"]["params"]["coords(dec. lat,long)"]
    lat_str, long_str = coords_str.split(',')
    lat = float(lat_str)
    long = float(long_str)
    coordinates = [long, lat]  #########reversed


    for reading in grouped_readings_list:
        
        temp_value = reading["TEMP"]
        cond_value = reading["COND"]
        ph_value = reading["PH"]
        ammonium_value = reading["AMMONIUM"]
        turbidity_value = reading["TURBIDITY"]
        doo_percent_value = reading["DOO PCENT"]

        date = reading["dateTime"]
        date_obj = datetime.strptime(date, '%d/%m/%Y %H:%M:%S GMT')
        date = date_obj.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+00:00Z'


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:WaterQualityObserved:UK-SWW-wq"+str(site_name)+"-siteId-"+str(site_id),
            "type": "WaterQualityObserved",
            "dateObserved": date,
            "location": {
                "type": "Point",
                "coordinates": coordinates
                },
            "dataProvider": "Meteor Communications Ltd",
            "temperature": temp_value,
            "conductivity": cond_value,
            "pH": ph_value,
            "NH4": ammonium_value,
            "turbidity": turbidity_value,
            "O2": doo_percent_value,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.WaterQuality/master/context.jsonld"
  ]
        }

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# UK - SWW - analog data ##################################################################################################

def convert_data_sww_analog(data):

    data_list = []

    for obj in data['values']:
        db_addr = str(obj["db_addr"])
        time = obj["time"]
        value = obj["value"]
     
        date = convert_time(time)

        if db_addr in data["stations"]:
            station = data["stations"][db_addr]
            lat = station["lat"]
            lon = station["lon"]
            site_name = station["Site_name"]
            asset_prefix = station["asset_prefix"]
            alarm_limit = station["AlarmLimit"]
            overflow_ename = station["Overflow_Ename_1"]

        coordinates = [lon, lat]
        site_name = site_name.replace(' ', '_')


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:DeviceMeasurement:UK-SWW-analog-"+str(site_name)+"-stationId-"+str(db_addr),
            "type": "DeviceMeasurement",
            "deviceType": "sensor",
            "controlledProperty": "waterLevel",
            "unit": "meter",
            "numValue": value,
            "dateObserved": date,
            "location": {
                "type": "Point",
                "coordinates": coordinates
                },
            "description": "AlarmLimit: "+ str(alarm_limit),
            "alternateName": asset_prefix,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld"
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list

# UK - SWW - digital data ##################################################################################################

def convert_data_sww_digital(data):

    data_list = []

    for obj in data['values']:
        db_addr = str(obj["db_addr"])
        time = obj["time"]
        value = obj["value"]

        date = convert_time(time)

        if db_addr in data["stations"]:
            station = data["stations"][db_addr]
            lat = station["lat"]
            lon = station["lon"]
            site_name = station["Site_name"]
            asset_prefix = station["asset_prefix"]
            alarm_limit = station["AlarmLimit"]
            overflow_ename = station["Overflow_Ename_1"]

        coordinates = [lon, lat]
        site_name = site_name.replace(' ', '_')


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:DeviceMeasurement:UK-SWW-digital-"+str(site_name)+"-stationId-"+str(db_addr),
            "type": "DeviceMeasurement",
            "deviceType": "sensor",
            "controlledProperty": "spillage",
            "numValue": value,
            "dateObserved": date,
            "location": {
                "type": "Point",
                "coordinates": coordinates
                },
            "description": "Pump running or not running, in some cases determine if a spill has occured",
            "alternateName": asset_prefix,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld"
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list

# UK - SWW - rainfall data ##################################################################################################

def convert_data_sww_rainfall(data):
    data_list = []

    for obj in data['values']:
        date = obj("index")
        edm = obj["edm"] #what is it? what is the unit?
        rain = obj["rain"]
        description = obj["outcomeTypeFilled"] #what is it?

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:DeviceMeasurement:UK-SWW-rainfall",
            "type": "DeviceMeasurement",
            "deviceType": "sensor",
            "controlledProperty": "rainfall",
            "unit": "millimeter", #???
            "numValue": edm,
            "textValue": rain,
            "dateObserved": date,
            "description": description,
            #location??
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld"
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list

# UK - SWW - Tavistock River level ##################################################################################################

def convert_data_sww_tavistock(data):

    data_list = []

    for obj in data['values']:
        date = obj("dateTime")
        level = obj["level"]

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:DeviceMeasurement:UK-SWW-Tavistock-level",
            "type": "DeviceMeasurement",
            "deviceType": "sensor",
            "measurementType": "level",
            "unit": "meter",
            "numValue": level,
            "dateObserved": date,
            #location??
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld"
  ]
        }
                           

        data_list.append(converted_data)

# DE - HST - rainfall ######################################################################################################

def convert_data_hst_rainfall_forecast(data):

    data_list = []

    for obj in data:

        value = obj['node_value']
        forecastPeriod = obj['node_id']
        coordinates = obj["coordinates"]
        current_date = datetime.now()
        formated_date = current_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:WeatherForecast:DE-HST-rainfall-forecast-OPCUA",
            "type": "WeatherForecast",
            "dataProvider": "HST",
            "dateCreated": formated_date,
            "description": forecastPeriod,
            "precipitation": value,
            "location": {
                "type": "Point",
                "coordinates": coordinates
                },
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Weather/master/context.jsonld"
  ]
        }

        data_list.append(converted_data)


    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list

# DE - HST - water observed ######################################################################################################

def convert_data_hst_waterObserved(data):

    data_list = []

    entity_id = data['entityId'].split(":")[-1]
    formated_date = None
    waterLevel = None
    coordinates = None

    for i in range(len(data['attributes'][0]['values'])):
        for obj in data['attributes']:

            if obj['attrName'] == 'dateObserved':
                dateObserved = obj['values'][i]
                date = datetime.fromisoformat(dateObserved[:-6])
                formated_date = date.strftime("%Y-%m-%dT%H:%M:%SZ")
            elif obj["attrName"] == 'waterLevel':
                waterLevel = obj['values'][i]

            if entity_id == 'eui-24e124126c144958':
                coordinates = [8.763568021931345, 51.6236771365379]
            elif entity_id == 'eui-24e124126c145086':
                coordinates = [8.765350447820591, 51.62637722143535]
            else: coordinates = None

            #convert data to ngsi-ld
            converted_data = {
                "id": "urn:ngsi-ld:WaterObserved:DE-HST-"+entity_id,
                "type": "WaterObserved",
                "dataProvider": "Etteln QI",
                "dateObserved": formated_date,
                "waterLevel": waterLevel,
                "location": {
                    "type": "Point",
                    "coordinates": coordinates
                    },
                "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/context.jsonld"
  ]
            }

        data_list.append(converted_data)


    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# DE - HST - weather observed ######################################################################################################

def convert_data_hst_weatherObserved(data):

    data_list = []

    entity_id = data['entityId'].split(":")[-1]
    formated_date, battery, ref_device, coordinates, precipitation, temperature  = None, None, None, None, None, None

    for i in range(len(data['attributes'][0]['values'])):
        for obj in data['attributes']:

            if obj['attrName'] == 'dateObserved':
                dateObserved = obj['values'][i]
                date = datetime.fromisoformat(dateObserved[:-4])
                formated_date = date.strftime("%Y-%m-%dT%H:%M:%SZ")
                
            elif obj['attrName'] == 'battery':
                battery = str(obj['values'][i])

            elif obj["attrName"] == 'location':
                ref_device = obj['values'][i]

            elif obj["attrName"] == 'location_centroid':
                coordinates_str = obj['values'][i]
                lat, lon = map(float, coordinates_str.split(','))
                coordinates = [lon, lat]

            elif obj["attrName"] == 'precipitation':
                precipitation = obj['values'][i]

            elif obj["attrName"] == 'temperature':
                temperature = obj['values'][i]
            


            #convert data to ngsi-ld
            converted_data = {
                "id": "urn:ngsi-ld:WeatherObserved:DE-HST-"+entity_id,
                "type": "WeatherObserved",
                "dataProvider": "Etteln QI",
                "dateObserved": formated_date,
                "description": "Weather observation data from Etteln QI about precipitation and temperature. Sensor battery level: "+battery,
                "refDevice": ref_device,
                "precipitation": precipitation,
                "temperature": temperature,
                "location": {
                    "type": "Point",
                    "coordinates": coordinates
                    },
                "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Weather/master/context.jsonld"
  ]
            }

        data_list.append(converted_data)


    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# DE - HST - flood monitoring ######################################################################################################

def convert_data_hst_floodMonitoring(data):

    data_list = []

    entity_id = data['entityId'].split(":")[-1]
    formated_date, battery, ref_device, coordinates, measuredDistance, referenceLevel, temperature  = None, None, None, None, None, None, None

    for i in range(len(data['attributes'][0]['values'])):
        for obj in data['attributes']:

            if obj['attrName'] == 'dateObserved':
                dateObserved = obj['values'][i]
                date = datetime.fromisoformat(dateObserved[:-6])
                formated_date = date.strftime("%Y-%m-%dT%H:%M:%SZ")
                
            elif obj['attrName'] == 'battery':
                battery = str(obj['values'][i])

            elif obj["attrName"] == 'location':
                ref_device = obj['values'][i]

            elif obj["attrName"] == 'location_centroid':
                coordinates_str = obj['values'][i]
                lat, lon = map(float, coordinates_str.split(','))
                coordinates = [lon, lat]

            elif obj["attrName"] == 'measuredDistance':
                measuredDistance = obj['values'][i]

            elif obj["attrName"] == 'referenceLevel':
                referenceLevel = obj['values'][i]

            elif obj["attrName"] == 'temperature':
                temperature = obj['values'][i]
            


            #convert data to ngsi-ld
            converted_data = {
                "id": "urn:ngsi-ld:FloodMonitoring:DE-HST-"+entity_id,
                "type": "FloodMonitoring",
                "dataProvider": "Etteln QI",
                "dateObserved": formated_date,
                "description": "Flood monitoring data from Etteln QI about temperature, referenceLevel and measuredDistance. Sensor battery level: "+battery,
                "refDevice": ref_device,
                "referenceLevel": referenceLevel,
                "temperature": temperature,
                "measuredDistance": measuredDistance,
                "location": {
                    "type": "Point",
                    "coordinates": coordinates
                    },
                "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/context.jsonld"
  ]
            }

        data_list.append(converted_data)


    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list

# CY - WBL - call complains ################################################################################################

def convert_data_wbl_call_complains(data):

    data_list = []

    for obj in data:
        serial = obj["serial"]
        topic = obj["topic"]
        reason = obj["reason"]
        date_taken_number = obj["date_taken_number"]
        time_taken = obj["time_taken"]
        date_completed = obj["date_completed"]
        time_to = obj["time_to"]
        taken_by = obj["taken_by"]
        comments = obj["comments"]
        area = obj["area"]
        given_to = obj["given_to"]
        action = obj["action"]
        municipality = obj["municipality"]
        street = obj["street"]
        number = obj["number"]
        status = obj["status"]
        postcode = obj["postcode"]
        people = obj["people"]
        building = obj["building"]
        lon_str = obj["lon"]
        lat_str = obj["lat"]

        if time_taken and date_taken_number:
            time_object = datetime.strptime(time_taken, "%H:%M")
            date_object = datetime.strptime(date_taken_number, "%Y%m%d")
            combined_datetime1 = datetime.combine(date_object.date(), time_object.time())
            date_taken = combined_datetime1.strftime('%Y-%m-%dT%H:%M:%S.000+00:00Z')
        else: date_taken = ""

        if date_completed and time_to:
            date_completed = datetime.strptime(date_completed, "%d/%m/%Y")
            time_to = datetime.strptime(time_to, "%H:%M")
            combined_datetime2 = datetime.combine(date_completed.date(), time_to.time())
            date_completed_formated = combined_datetime2.strftime('%Y-%m-%dT%H:%M:%S.000+00:00Z')
        else: date_completed_formated = ""

        if lat_str and lon_str:
            lat = float(lat_str)
            lon = float(lon_str)
            coordinates = [lon, lat]
        else: coordinates = [None, None]

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:Complaint:CY-WBL-callComplaints-area-"+area,
            "type": "Complaint",
            "category": action,
            "areaServed": building,
            "alternateName": reason,
            "description": comments,
            "timestamp": date_taken,
            "dateModified": date_completed_formated,
            "location": {
                "type": "Point",
                "coordinates": coordinates
                },
            "status":status,
            "isFiledTo": given_to + ", " + taken_by,
            "isMadeBy": people,
            "address": {
                "addressLocality": municipality,
                "postalCode": postcode,
                "streetAddress": street + ", " + number,
            },
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.CallComplaints/master/context.jsonld"
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# CY - WBL - telemetry #####################################################################################################

def convert_data_wbl_telemetry(data):

    data_list = []

    for obj in data:
        serial = obj["serial"]
        node_id = obj["node_id"]
        timestamp_str = obj["timestamp"]
        volume = obj["volume"]
        flow = obj["flow"]
        volume = float(volume)
        flow = float(flow)

        timestamp = datetime.strptime(timestamp_str, "%d/%m/%Y %H:%M")
        formatted_date = timestamp.strftime('%Y-%m-%dT%H:%M:%S.000+00:00Z')

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:WaterObserved:CY-WBL-telemetryData-DMA131",
            "type": "WaterObserved",
            "description": "Daily telemetry data from WBL DMA131. ObjectVolume: m^3. Flow: m^3/h",
            "dataProvider": "WBL",
            "refDevice": node_id,
            "location": {
                "type": "Point",
                "coordinates": [34.681372,33.045627]
                },
            "objectVolume": volume,
            "flow": flow,
            "dateObserved": formatted_date,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/context.jsonld"
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# CY - WBL - smart metering #####################################################################################################

def convert_data_wbl_smart_metering(data):

    data_list = []

    for obj in data:
        serial = obj["serial"]
        device_id = obj["device_id"]
        groupname = obj["groupname"]
        medium = obj["medium"]
        timestamp_str = obj["timestamp"]
        value = obj["value"]
        value = float(value)

        timestamp = datetime.strptime(timestamp_str, "%d/%m/%Y %H:%M")
        formatted_date = timestamp.strftime('%Y-%m-%dT%H:%M:%S.000+00:00Z')

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:DeviceMeasurement:CY-WBL-smartMeteringData-"+groupname,
            "type": "DeviceMeasurement",
            "description": "Volume of water measurement",
            "dataProvider": "WBL",
            "refDevice": device_id,
            "numValue": value,
            "unit": "m^3",
            "dateObserved": formatted_date,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld"
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# CY - WBL - telemetry 2 #####################################################################################################
def convert_data_wbl_telemetry_new(data):

    data_list = []

    for obj in data:
        timestamp = obj["time"]
        flow = obj["value"]
        sensorid = obj["sensorid"]
        sensortype = obj["sensortype"]
        flow = float(flow)

        lat = 34.68071062318811
        long = 32.986225581023504
        coordinates = [long, lat]

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:WaterObserved:CY-WBL-telemetryData-version2",
            "type": "WaterObserved",
            "description": "Sensor: " + sensorid + ". Type: " + sensortype + ". Daily telemetry data from WBL. Flow: m^3/h",
            "dataProvider": "WBL",
            "location": {
                "type": "Point",
                "coordinates": coordinates
                },
            "flow": flow,
            "dateObserved": timestamp,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/context.jsonld"
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# FI - KEY - water duct #####################################################################################################

def convert_data_key_water_duct(data):

    results = data["Results"]
    data_list = []
    transformer = Transformer.from_crs("EPSG:3881", "EPSG:4326", always_xy=True)

    for obj in results:
        mslink = obj["mslink"]
        created_ts = obj["created_ts"]
        updated_ts = obj["updated_ts"]
        length = obj["length"]
        sym_code_gt = obj["sym_code_gt"]
        sym_name = obj["sym_name"]
        begin_node_id = obj["begin_node_id"]
        end_node_id = obj["end_node_id"]
        diameter = obj["Type"]["diameter"]
        material_txt = obj["Type"]["material_txt"]
        epanet_roughness = obj["epanet_roughness"]
        owner = obj["owner"]["txt"]

        if created_ts is not None:
            parsed_created_ts = datetime.strptime(created_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_created_ts = None

        if updated_ts is not None:
            parsed_updated_ts = datetime.strptime(updated_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_updated_ts = None
        
        location_str = obj["location"]
        coordinates = []

        if location_str is not None:
            points = location_str.split('(')[1].split(')')[0].split(',')
            for point in points:
                coords = [float(coord.strip()) for coord in point.split()]
                coordinates.append(coords)

            transformed_coordinates = []
            for coord in coordinates:
                x, y, z = coord
                lon, lat = transformer.transform(x, y)
                transformed_coordinates.append((lon, lat, z))
        else: coordinates = None

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:Pipe:FI-KEY-waterDuctData-"+sym_code_gt+"-"+sym_name,
            "type": "Pipe",
            "description": "mslink: "+ str(mslink),
            "dataProvider": "Keyaqua",
            "dateCreated": parsed_created_ts,
            "dateModified": parsed_updated_ts,
            "diameter": diameter,
            "startsAt": begin_node_id,
            "endsAt": end_node_id,
            "length": length,
            "roughness": epanet_roughness,
            "tag": material_txt,
            "owner": owner,
            "location": {
                "type": "LineString",
                "coordinates": transformed_coordinates
                },
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.WaterDistributionManagementEPANET/master/context.jsonld"
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# FI - KEY - water node #####################################################################################################

def convert_data_key_water_node(data):

    results = data["Results"]
    data_list = []
    transformer = Transformer.from_crs("EPSG:3881", "EPSG:4326", always_xy=True)

    for obj in results:
        mslink = obj["mslink"]
        created_ts = obj["created_ts"]
        updated_ts = obj["updated_ts"]
        updated_by = obj["updated_by"]
        node_type = obj["node_type"]
        feature = obj["feature"]
        network_type = obj["valve_type"]["network_type"]["txt"]
        valve_type = obj["valve_type"]["valve_type"]["txt"]
        sym_code_gt = obj["sym_code_gt"]
        sym_name = obj["sym_name"]
        owner = obj["owner"]["txt"]
        angle = obj["angle"]

        if created_ts is not None:
            parsed_created_ts = datetime.strptime(created_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_created_ts = None

        if updated_ts is not None:
            parsed_updated_ts = datetime.strptime(updated_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_updated_ts = None
        
        location_str = obj["location"]
        if location_str is not None:
            coordinates_str = location_str.split("(")[1]
            coordinates = coordinates_str.split(" ")[0:3]
            lon1 = float(coordinates[0])
            lat1 = float(coordinates[1])
            alt = float(coordinates[2].strip(")"))
            lon, lat = transformer.transform(lon1, lat1)
            coordinates_final = [lon, lat, alt]
        else: coordinates_final = None


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:Node:FI-KEY-waterNodeData-"+sym_code_gt+"-"+sym_name,
            "type": "Node",
            "mslink": mslink,
            "dataProvider": "Keyaqua",
            "dateCreated": parsed_created_ts,
            "dateModified": parsed_updated_ts,
            "updatedBy": updated_by,
            "nodeType": node_type,
            "feature": feature,
            "networkType": network_type,
            "valveType": valve_type,
            "angle": angle,
            "owner": owner,
            "location": {
                "type": "Point",
                "coordinates": coordinates_final
                },
            "@context": None
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# FI - KEY - duct conditions #####################################################################################################

def convert_data_key_duct_conditions(data):

    results = data["Results"]
    data_list = []

    for obj in results:
        structural_condition = obj["structural_condition"]
        functional_condition = obj["functional_condition"]
        leak_condition = obj["leak_condition"]
        leak_index = obj["leak_index"]
        significance = obj["significance"]
        fault_code = obj["fault_code"]
        condition_index = obj["condition_index"]
        selected = obj["selected"]
        source = obj["source"]
        sewer_duct_id = obj["sewer_duct_id"]
        water_duct_id = obj["water_duct_id"]
        verbal_condition_info = obj["verbal_condition_info"]


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:Pipe:FI-KEY-ductConditions",
            "type": "Pipe",
            "description": "Keyaqua water duct condition",
            "owner": "Keyaqua",
            "structuralCondition": structural_condition,
            "functionalCondition": functional_condition,
            "leakCondition": leak_condition,
            "leakIndex": leak_index,
            "significance": significance,
            "faultCode": fault_code,
            "conditionIndex": condition_index,
            "selected": selected,
            "source": source,
            "sewerDuctId": sewer_duct_id,
            "waterDuctId": water_duct_id,
            "verbalConditionInfo": verbal_condition_info,
            "@context": None
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# FI - KEY - sewer pumping station #####################################################################################################

def convert_data_key_sewer_pumping_station(data):

    results = data["Results"]
    data_list = []
    transformer = Transformer.from_crs("EPSG:3881", "EPSG:4326", always_xy=True)

    for obj in results:
        mslink = obj["mslink"]
        created_ts = obj["created_ts"]
        node_type = obj["node_type"]
        feature = obj["pump_type"]["feature"]
        network = obj["network"]
        owner = obj["owner"]["txt"]
        net_type = obj["pump_type"]["net_type"]["txt"]
        network_type = obj["pump_type"]["network_type"]["txt"]
        pump_type = obj["pump_type"]["pump_type"]["txt"]
        sym_angle = obj["sym_angle"]
        sym_code_gt = obj["sym_code_gt"]
        sym_name = obj["sym_name"]
        sym_name_dwg = obj["sym_name_dwg"]
        sym_size = obj["sym_size"]
        sym_width = obj["sym_width"]
        updated_by = obj["updated_by"]
        updated_ts = obj["updated_ts"]
        usage_state = obj["usage_state"]["txt"]
        

        if created_ts is not None:
            parsed_created_ts = datetime.strptime(created_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_created_ts = None

        if updated_ts is not None:
            parsed_updated_ts = datetime.strptime(updated_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_updated_ts = None
        
        location_str = obj["location"]
        if location_str is not None:
            coordinates_str = location_str.split("(")[1]
            coordinates = coordinates_str.split(" ")[0:3]
            lon1 = float(coordinates[0])
            lat1 = float(coordinates[1])
            alt = float(coordinates[2].strip(")"))
            lon, lat = transformer.transform(lon1, lat1)
            coordinates_final = [lon, lat, alt]
        else: coordinates_final = None


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:Pump:FI-KEY-sewerPumpingStation-"+sym_code_gt+"-"+sym_name+"-"+sym_name_dwg,
            "type": "Pump",
            "mslink": mslink,
            "dataProvider": "Keyaqua",
            "dateCreated": parsed_created_ts,
            "dateModified": parsed_updated_ts,
            "updatedBy": updated_by,
            "nodeType": node_type,
            "feature": feature,
            "networkType": network_type,
            "network": network,
            "netType": net_type,
            "pumpType": pump_type,
            "symAngle": sym_angle,
            "symSize": sym_size,
            "symWidth": sym_width,
            "usageState": usage_state,
            "owner": owner,
            "location": {
                "type": "Point",
                "coordinates": coordinates_final
                },
            "@context": None
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# FI - KEY - sewer valve #####################################################################################################

def convert_data_key_sewer_valve(data):

    results = data["Results"]
    data_list = []
    transformer = Transformer.from_crs("EPSG:3881", "EPSG:4326", always_xy=True)

    for obj in results:
        mslink = obj["mslink"]
        created_ts = obj["created_ts"]
        node_type = obj["node_type"]
        feature = obj["feature"]
        network = obj["network"]
        owner = obj["owner"]["txt"]
        net_type = obj["valve_type"]["net_type"]["txt"]
        network_type = obj["valve_type"]["network_type"]["txt"]
        pump_type = obj["valve_type"]["valve_type"]["txt"]
        angle = obj["angle"]
        mapping_method = obj["mapping_method"]["txt"]
        survey_number = obj["survey_number"]
        # sym_code_gt = obj["sym_code_gt"]
        sym_name = obj["sym_name"]
        sym_name_dwg = obj["sym_name_dwg"]
        sym_size = obj["sym_size"]
        sym_width = obj["sym_width"]
        updated_by = obj["updated_by"]
        updated_ts = obj["updated_ts"]
        usage_state = obj["usage_state"]["txt"]

        

        if created_ts is not None:
            parsed_created_ts = datetime.strptime(created_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_created_ts = None

        if updated_ts is not None:
            parsed_updated_ts = datetime.strptime(updated_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_updated_ts = None
        
        location_str = obj["location"]
        if location_str is not None:
            coordinates_str = location_str.split("(")[1]
            coordinates = coordinates_str.split(" ")[0:3]
            lon1 = float(coordinates[0])
            lat1 = float(coordinates[1])
            alt = float(coordinates[2].strip(")"))
            lon, lat = transformer.transform(lon1, lat1)
            coordinates_final = [lon, lat, alt]
        else: coordinates_final = None


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:Valve:FI-KEY-sewerValve-"+sym_name+"-"+sym_name_dwg,
            "type": "Valve",
            "mslink": mslink,
            "dataProvider": "Keyaqua",
            "dateCreated": parsed_created_ts,
            "dateModified": parsed_updated_ts,
            "updatedBy": updated_by,
            "nodeType": node_type,
            "feature": feature,
            "networkType": network_type,
            "network": network,
            "netType": net_type,
            "pumpType": pump_type,
            "angle": angle,
            "mappingMethod": mapping_method,
            "surveyNumber": survey_number,
            "symSize": sym_size,
            "symWidth": sym_width,
            "usageState": usage_state,
            "owner": owner,
            "location": {
                "type": "Point",
                "coordinates": coordinates_final
                },
            "@context": None
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# FI - KEY - flow meters #####################################################################################################

def convert_data_key_flow_meters(data):

    results = data["Results"]
    data_list = []
    transformer = Transformer.from_crs("EPSG:3881", "EPSG:4326", always_xy=True)

    for obj in results:
    
        flow_number = obj["flow_number"]
        module_number = obj["module_number"]
        Size_id = obj["Size_id"]["txt"]
        Size_value = obj["Size_id"]["value"]
        created_ts = obj["Consumer_id"]["created_ts"]
        updated_ts = obj["Consumer_id"]["updated_ts"]
        updated_by = obj["Consumer_id"]["updated_by"]
        cp_type_txt = obj["Consumer_id"]["cp_type"]["txt"]
        cp_type_value = obj["Consumer_id"]["cp_type"]["num_value"]
        cp_state = obj["Consumer_id"]["cp_state"]["txt"]
        Flow_type_id = obj["Flow_type_id"]["txt"]
        Class_id = obj["Class_id"]["txt"]
        

        if created_ts is not None:
            parsed_created_ts = datetime.strptime(created_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_created_ts = None

        if updated_ts is not None:
            parsed_updated_ts = datetime.strptime(updated_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_updated_ts = None
        
        location_str = obj["Consumer_id"]["location"]
        if location_str is not None:
            coordinates_str = location_str.split("(")[1]
            coordinates = coordinates_str.split(" ")[0:3]
            lon1 = float(coordinates[0])
            lat1 = float(coordinates[1])
            alt = float(coordinates[2].strip(")"))
            lon, lat = transformer.transform(lon1, lat1)
            coordinates_final = [lon, lat, alt]
        else: coordinates_final = None


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:FlowMeter:FI-KEY-flowMeter-"+flow_number,
            "type": "FlowMeter",
            "dataProvider": "Keyaqua",
            "dateCreated": parsed_created_ts,
            "dateModified": parsed_updated_ts,
            "updatedBy": updated_by,
            "moduleNumber": module_number,
            "flowMeterSizeId": Size_id,
            "flowMeterSizeValue": Size_value,
            "consumerPointType": cp_type_txt,
            "consumerPointValue": cp_type_value,
            "consumerPointState": cp_state,
            "flowMeterType": Flow_type_id,
            "flowMeterClass": Class_id,
            "location": {
                "type": "Point",
                "coordinates": coordinates_final
                },
            "@context": None
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# FI - KEY - flow meter reading #####################################################################################################

def convert_data_key_flow_meter_reading(data):

    results = data["Results"]
    data_list = []

    for obj in results:
    
        reading = obj["reading"]
        reading_date = obj["reading_date"]
        consumption_estimate = obj["consumption_estimate"]
        flow_meter_value = obj["FlowMeter_id"]

        
        id = None
        flow_number = None
        install_date = None
        module_number = None
        Size_id = None
        Consumer_id = None
        Flow_type_id = None
        Class_id = None
        Water_type_id = None

        if isinstance(flow_meter_value, dict):
            id = obj["FlowMeter_id"]["id"]
            flow_number = obj["FlowMeter_id"]["flow_number"]
            install_date = obj["FlowMeter_id"]["install_date"]
            module_number = obj["FlowMeter_id"]["module_number"]
            Size_id = obj["FlowMeter_id"]["Size_id"]
            Consumer_id = obj["FlowMeter_id"]["Consumer_id"]
            Flow_type_id = obj["FlowMeter_id"]["Flow_type_id"]
            Class_id = obj["FlowMeter_id"]["Class_id"]
            Water_type_id = obj["FlowMeter_id"]["Water_type_id"]
        
        

        if reading_date is not None:
            try:
                parsed_reading_date = datetime.strptime(reading_date, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                parsed_reading_date = datetime.strptime(reading_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_reading_date = None

        if install_date is not None:
            try:
                parsed_install_date = datetime.strptime(install_date, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                parsed_install_date = datetime.strptime(install_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_install_date = None
        


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:FlowMeterReading:FI-KEY-flowMeterReading",
            "type": "FlowMeterReading",
            "dataProvider": "Keyaqua",
            "reading": reading,
            "flowNumber": flow_number,
            "readingDate": parsed_reading_date,
            "installDate": parsed_install_date,
            "consumptionEstimate": consumption_estimate,
            "flowMeterId": id,
            "moduleNumber": module_number,
            "sizeId": Size_id,
            "consumerId": Consumer_id,
            "flowTypeId": Flow_type_id,
            "classId": Class_id,
            "waterTypeId": Water_type_id,
            "@context": None
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# FI - KEY - sewer manhole #####################################################################################################

def convert_data_key_sewer_manhole(data):

    results = data["Results"]
    data_list = []
    transformer = Transformer.from_crs("EPSG:3881", "EPSG:4326", always_xy=True)

    for obj in results:
    
        angle = obj["angle"]
        created_ts = obj["created_ts"]
        feature = obj["feature"]
        mslink = obj["mslink"]
        network = obj["network"]
        node_type = obj["node_type"]
        pressure = obj["pressure"]
        sym_code_gt = obj["sym_code_gt"]
        sym_name = obj["sym_name"]
        sym_name_dwg = obj["sym_name_dwg"]
        sym_size = obj["sym_size"]
        sym_width = obj["sym_width"]
        updated_by = obj["updated_by"]
        updated_ts = obj["updated_ts"]
        manhole_material = obj["manhole"]["material"]["txt"]
        net_type = obj["manhole_type"]["net_type"]["txt"]
        network_type = obj["manhole_type"]["network_type"]["txt"]
        manhole_type = obj["manhole_type"]["manhole_type"]["txt"]
        lid_type = obj["manhole_type"]["lid_type"]["txt"]
        mapping_method = obj["mapping_method"]["txt"]
        owner = obj["owner"]["txt"]
        usage_state = obj["usage_state"]["txt"]
        
        

        if created_ts is not None:
            parsed_created_ts = datetime.strptime(created_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_created_ts = None

        if updated_ts is not None:
            parsed_updated_ts = datetime.strptime(updated_ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            parsed_updated_ts = None
        
        location_str = obj["location"]
        if location_str is not None:
            coordinates_str = location_str.split("(")[1]
            coordinates = coordinates_str.split(" ")[0:3]
            lon1 = float(coordinates[0])
            lat1 = float(coordinates[1])
            alt = float(coordinates[2].strip(")"))
            lon, lat = transformer.transform(lon1, lat1)
            coordinates_final = [lon, lat, alt]
        else: coordinates_final = None


        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:sewerManhole:FI-KEY-sewerManhole-"+sym_code_gt+"-"+sym_name+"-"+sym_name_dwg,
            "type": "sewerManhole",
            "dataProvider": "Keyaqua",
            "dateCreated": parsed_created_ts,
            "dateModified": parsed_updated_ts,
            "updatedBy": updated_by,
            "angle": angle,
            "feature": feature,
            "mslink": mslink,
            "network": network,
            "nodeType": node_type,
            "pressure": pressure,
            "symSize": sym_size,
            "symWidth": sym_width,
            "manholeMaterial": manhole_material,
            "netType": net_type,
            "networkType": network_type,
            "manholeType": manhole_type,
            "lidType": lid_type,
            "mappingMethod": mapping_method,
            "owner": owner,
            "usageState": usage_state,
            "location": {
                "type": "Point",
                "coordinates": coordinates_final
                },
            "@context": None
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list

# FI - KEY - T3-T11 #####################################################################################################

def convert_data_key_t3_t11(data):

    data_list = []
    transformer = Transformer.from_crs("EPSG:3067", "EPSG:4326", always_xy=True)

    for obj in data:
    
        surfaceCode = obj["surface_code"]
        lineNumber = obj["line_number"]
        surveyCode = obj["survey_code"]
        pointNumber = obj["point_number"]
        northCoordinate = obj["nort_coord"]
        eastCoordinate = obj["east_coord"]
        z = obj["z"]
        depth = obj.get("depth")
        locationAccuracy = obj.get("location_accuracy")
        heightAccuracy = obj.get("height_accuracy")
        material = obj.get("material")
        diameter = obj.get("diameter")
        comment = obj.get("comment")

        east = float(eastCoordinate)
        north = float(northCoordinate)
        #print("East:", east, "North:", north)
        lon, lat = transformer.transform(east, north)
        #print("Transformed coordinates:", lon, lat)
        coordinates_final = [lon, lat]



        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:sewerManhole:FI-KEY-sewerManhole-T3-T11-SurfaceCode: "+surfaceCode,
            "type": "sewerManhole",
            "dataProvider": "Keyaqua",
            "lineNumber": lineNumber,
            "surveyCode": surveyCode,
            "pointNumber": pointNumber,
            "z": z,
            "depth": depth,
            "locationAccuracy": locationAccuracy,
            "heightAccuracy": heightAccuracy,
            "material": material,
            "diameter": diameter,
            "comment": comment,
            "location": {
                "type": "Point",
                "coordinates": coordinates_final
                },
            "@context": None
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list

# FI - KEY - CGI #####################################################################################################

def convert_data_key_cgi(data):

    data_list = []

    for obj in data:
    
        customer = obj["customer(k)"]
        name = obj["name"]
        address = obj["address"]
        postCode = obj["posti"]
        city = obj["city"]
        phone = obj["phone"]
        meter = obj["meter"]
        payer = obj["payer"]
        recipient = obj["reciepient"]

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:sewerManhole:FI-KEY-CGI",
            "type": "customerDetails",
            "dataProvider": "Keyaqua",
            "customer": customer,
            "name": name,
            "address": address,
            "postCode": postCode,
            "city": city,
            "phone": phone,
            "meter": meter,
            "payer": payer,
            "recipient": recipient,
            "@context": None
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list

# FI - KEY - Usecase 3 #####################################################################################################

def convert_data_key_usecase_3(data):
    data_list = []

    for obj in data:
    
        name = obj["name"]
        st_astext = obj["st_astext"]
        originalId = obj["origina_id"]
        time = obj["time"]
        condition = obj["condition"]
        comment1 = obj["comment1"]
        comment2 = obj["comment2"]
        comment3 = obj["comment3"]
        comment4 = obj["comment4"]
        followUp = obj["followUp"]
        followUpDate = obj["followUpDate"]
        fileName = obj["fileName"]

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:sewerManhole:FI-KEY-U3",
            "type": "customerDetails",
            "dataProvider": "Keyaqua",
            "name": name,
            "st_astext": st_astext,
            "originalId": originalId,
            "time": time,
            "condition": condition,
            "comment1": comment1,
            "comment2": comment2,
            "comment3": comment3,
            "comment4": comment4,
            "followUp": followUp,
            "followUpDate": followUpDate,            
            "fileName": fileName,
            "@context": None
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list

# SP - HIDR - snowflake SQL-1 #####################################################################################################

def convert_data_hidr_snowflake_sql_1(data):

    data_list = []

    for obj in data:
        
        TENANTNAME = obj["TENANTNAME"]
        SITENAME = obj["SITENAME"]
        METERSERIAL = obj["METERSERIAL"]
        DEVICESERIAL = obj["DEVICESERIAL"]
        CONTRACTKEY = obj["CONTRACTKEY"]
        SECTOR = obj["SECTOR"]
        SUBSECTOR = obj["SUBSECTOR"]
        CONTROL = obj["CONTROL"]
        GGCC = obj["GGCC"]
        SAMPLETIME = obj["SAMPLETIME"]
        READING = obj["READING"]
        CONSUMPTION = obj["CONSUMPTION"]

        reading_str = str(READING)
        
        try:
            date_obj = datetime.strptime(SAMPLETIME, "%d/%m/%Y %H:%M:%S")
            converted_date = date_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            converted_date = None

        if CONTROL.lower() == "true":
            deviceType = "controlMeter"
        elif GGCC.lower() == "true":
            deviceType = "largeConsumerMeter"
        else:
            deviceType = "defaultMeter"
 

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:DeviceMeasurement:SP-HIDR-snowflakeData-SQL-1-"+SITENAME,
            "type": "DeviceMeasurement",
            "description": "Accumulated Meter Consumption: "+reading_str,
            "dataProvider": TENANTNAME,
            "areaServed": SITENAME,
            "owner": [
                "deviceSerial: "+DEVICESERIAL,
                "contractKey: "+CONTRACTKEY,
                "sector: "+SECTOR
            ],
            "refDevice": METERSERIAL,
            "deviceType": deviceType,
            "numValue": CONSUMPTION,
            "unit": "m^3",
            "dateObserved": converted_date,
            "@context": [
    "https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld"
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# SP - HIDR - snowflake SQL-2 #####################################################################################################

def convert_data_hidr_snowflake_sql_2(data):

    data_list = []

    for obj in data:
        
        TENANTNAME = obj["TENANTNAME"]
        SITENAME = obj["SITENAME"]
        SECTOR = obj["SECTOR"]
        SAMPLEDATE = obj["SAMPLEDATE"]
        CONSUMPTION = obj["CONSUMPTION"]
        COUNTMETERS = obj["COUNTMETERS"]
        
        try:
            date_obj = datetime.strptime(SAMPLEDATE, '%d/%m/%Y')
            converted_date = date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            converted_date = None
            

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:WaterConsumption:SP-HIDR-snowflakeData-SQL-2-"+SITENAME,
            "type": "WaterConsumption",
            "description": "Water consumption registered by the smart meters of each hydraulic sector",
            "dataProvider": TENANTNAME,
            "areaServed": SITENAME,
            "waterConsumption": CONSUMPTION,
            "sector": SECTOR,
            "countMeters": COUNTMETERS,
            "sampleDate": converted_date,
            "@context": [
                None
            ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# SP - HIDR - hydraulic efficiency #####################################################################################################

def convert_data_hidr_hydraulic_efficiency(data):

    data_list = []

    for obj in data:
        
        Regional_Directorate = obj["Regional Directorate"]
        Concessions_Management = obj["Concessions Management"]
        Operation_Unit = obj["Operation Unit"]
        Activity = obj["Activity"]
        Year = obj["Year"]
        Month = obj["Month"]
        Volume_Customers = obj["Volume of potable registered water for Customers"]
        Volume_Municipalities = obj["Volume of potable registered water for Municipalities"]
        Volume_Total_consumption = obj["Volume of potable registered water (Total authorized consumption)"]
        Month_name = obj["Month_name"]
        Volume_billed_Total = obj["Volume of potable billed water (Total)"]
        
        last_day = calendar.monthrange(Year, Month)[1]
        date = f"{Year}-{Month:02d}-{last_day:02d}T00:00:00Z"

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:HydraulicEfficiency:SP-HIDR-hydraulicEfficiency-"+Operation_Unit,
            "type": "HydraulicEfficiency",
            "dataProvider": "Hidralia",
            "regionalDirectorate": Regional_Directorate,
            "concessionsManagement": Concessions_Management,
            "activity": Activity,
            "volumeCustomers": string_to_float(Volume_Customers),
            "volumeMunicipalities": string_to_float(Volume_Municipalities),
            "volumeTotalConsumption": string_to_float(Volume_Total_consumption),
            "volumeBilledTotal": string_to_float(Volume_billed_Total),
            "monthName": Month_name,
            "dateCreated": date,
            "@context": [
    None
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# SP - HIDR - Leaks M #####################################################################################################

def convert_data_hidr_leaks(data):

    data_list = []

    for obj in data:
        
        Regional_Directorate = obj["Regional Directorate"]
        Concessions_Management = obj["Concessions Management"]
        Operation_Unit = obj["Operation Unit"]
        Activity = obj["Activity"]
        Year = obj["Year"]
        Month = obj["Month"]
        visible_leaks_distribution = obj["Number of visible leaks in distribution network pipes (Total)"]
        visible_leaks_transmission = obj["Number of visible leaks in transmission network pipes (Total)"]
        natural_leaks_distribution = obj["Number of natural visible leaks in distribution network pipes "]
        natural_leaks_transmission = obj["Number of natural visible leaks in transmission network pipes"]
        provoked_leaks_distribution = obj["Number of provoked visible leaks in distribution network pipes"]
        provoked_leaks_transmission = obj["Number of provoked visible leaks in transmission network pipes"]
        hidden_leaks_service = obj["Number of hidden leaks in service connections"]
        hidden_leaks_pipes = obj["Number of hidden leaks in pipes"]
        natural_leaks_service = obj["Number of natural visible leaks in service connections"]
        provoked_leaks_service = obj["Number of provoked visible leaks in service connections"]
        Month_name = obj["Month_name"]
        total_length_network = obj["Total length of supply network (km)"]
        
        last_day = calendar.monthrange(Year, Month)[1]
        date = f"{Year}-{Month:02d}-{last_day:02d}T00:00:00Z"

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:Leaks:SP-HIDR-leaksM-"+Operation_Unit,
            "type": "Leaks",
            "dataProvider": "Hidralia",
            "regionalDirectorate": Regional_Directorate,
            "concessionsManagement": Concessions_Management,
            "activity": Activity,
            "visibleLeaksDistribution": string_to_float(visible_leaks_distribution),
            "visibleLeaksTransmission": string_to_float(visible_leaks_transmission),
            "naturalLeaksDistribution": string_to_float(natural_leaks_distribution),
            "naturalLeaksTransmission": string_to_float(natural_leaks_transmission),
            "provokedLeaksDistribution": string_to_float(provoked_leaks_distribution),
            "provokedLeaksTransmission": string_to_float(provoked_leaks_transmission),
            "hiddenLeaksService": string_to_float(hidden_leaks_service),
            "hiddenLeaksPipes": string_to_float(hidden_leaks_pipes),
            "naturalLeaksService": string_to_float(natural_leaks_service),
            "provokedLeaksService": string_to_float(provoked_leaks_service),
            "totalLengthNetwork": string_to_float(total_length_network),
            "monthName": Month_name,
            "dateCreated": date,
            "@context": [
    None
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# SP - HIDR - Energy #####################################################################################################

def convert_data_hidr_energy(data):

    data_list = []

    for obj in data:
        
        Regional_Directorate = obj["Dirección Regional"]
        Concessions_Management = obj["Gerencia de Concesiones"]
        Operation_Unit = obj["Explotación"]
        Activity = obj["Actividad"]
        Year = obj["Año"]
        Month = obj["Mes numero"]
        Billed_electricity = obj["Billed electricity"]
        Billed_electricity_P1 = obj["Billed electricity P1"]
        Billed_electricity_P2 = obj["Billed electricity P2"]
        Billed_electricity_P3 = obj["Billed electricity P3"]
        Billed_electricity_P4 = obj["Billed electricity P4"]
        Billed_electricity_P5 = obj["Billed electricity P5"]
        Billed_electricity_P6 = obj["Billed electricity P6"]
        Total_recorded_electricity_M = obj["Total recorded electricity - GA (M)"]
        Total_electricity_fed_M = obj["Total electricity fed into the grid - GA (M)"]
        Total_self_consumed_electricity = obj["Total self-consumed electricity (M)"]
        Total_renewable_energy_produced = obj["Total renewable energy produced (M)"]
        Average_purchase_energy_price = obj["Average purchase price of energy"]
        Average_purchase_energy_price_P1 = obj["Average purchase price of energy P1"]
        Average_purchase_energy_price_P2 = obj["Average purchase price of energy P2"]
        Average_purchase_energy_price_P3 = obj["Average purchase price of energy P3"]
        Average_purchase_energy_price_P4 = obj["Average purchase price of energy P4"]
        Average_purchase_energy_price_P5 = obj["Average purchase price of energy P5"]
        Average_purchase_energy_price_P6 = obj["Average purchase price of energy P6"]
        Total_billed_cost = obj["Total billed cost"]
        Unit_billed_cost_M = obj["Unit billed cost (M)"]
        Total_billed_cost_power_excesses = obj["Total billed cost for power excesses"]
        Power_excesses = obj["Power excesses"]
        Average_cost_of_energy_term = obj["Average cost of the energy term (/kWh)"]
        Total_billed_cost_of_energy_term = obj["Total billed cost of the energy term"]
        Unit_billed_cost_of_energy_term = obj["Unit billed cost of the energy term (M)"]
        total_billed_cost_of_energy_term_with_power_excesses = obj["Total billed cost of the power term, including power excesses (M)"]
        unit_billed_cost_of_energy_term_with_power_excesses = obj["Unit billed cost of the power term, including power excesses (M)"]
        Month_name = obj["Mes nombre"]
        
        last_day = calendar.monthrange(Year, Month)[1]
        date = f"{Year}-{Month:02d}-{last_day:02d}T00:00:00Z"

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:Leaks:SP-HIDR-leaksM-"+Operation_Unit,
            "type": "Leaks",
            "dataProvider": "Hidralia",
            "regionalDirectorate": Regional_Directorate,
            "concessionsManagement": Concessions_Management,
            "activity": Activity,
            "billedElectricity": string_to_float(Billed_electricity),
            "billedElectricityP1": string_to_float(Billed_electricity_P1),
            "billedElectricityP2": string_to_float(Billed_electricity_P2),
            "billedElectricityP3": string_to_float(Billed_electricity_P3),
            "billedElectricityP4": string_to_float(Billed_electricity_P4),
            "billedElectricityP5": string_to_float(Billed_electricity_P5),
            "billedElectricityP6": string_to_float(Billed_electricity_P6),
            "totalRecordedElectricityM": string_to_float(Total_recorded_electricity_M),
            "totalElectricityFedM": string_to_float(Total_electricity_fed_M),
            "totalSelfConsumedElectricity": string_to_float(Total_self_consumed_electricity),
            "totalRenewableEnergyProduced": string_to_float(Total_renewable_energy_produced),
            "averagePurchaseEnergyPrice": string_to_float(Average_purchase_energy_price),
            "averagePurchaseEnergyPriceP1": string_to_float(Average_purchase_energy_price_P1),
            "averagePurchaseEnergyPriceP2": string_to_float(Average_purchase_energy_price_P2),
            "averagePurchaseEnergyPriceP3": string_to_float(Average_purchase_energy_price_P3),
            "averagePurchaseEnergyPriceP4": string_to_float(Average_purchase_energy_price_P4),
            "averagePurchaseEnergyPriceP5": string_to_float(Average_purchase_energy_price_P5),
            "averagePurchaseEnergyPriceP6": string_to_float(Average_purchase_energy_price_P6),
            "totalBilledCost": string_to_float(Total_billed_cost),
            "unitBilledCostM": string_to_float(Unit_billed_cost_M),
            "totalBilledCostPowerExcesses": string_to_float(Total_billed_cost_power_excesses),
            "powerExcesses": string_to_float(Power_excesses),
            "averageCostOfEnergyTerm": string_to_float(Average_cost_of_energy_term),
            "totalBilledCostOfEnergyTerm": string_to_float(Total_billed_cost_of_energy_term),
            "unitBilledCostOfEnergyTerm": string_to_float(Unit_billed_cost_of_energy_term),
            "totalBilledCostOfEnergyTermWithPowerExcesses": string_to_float(total_billed_cost_of_energy_term_with_power_excesses),
            "unitBilledCostOfEnergyTermWithPowerExcesses": string_to_float(unit_billed_cost_of_energy_term_with_power_excesses),
            "monthName": Month_name,
            "dateCreated": date,
            "@context": [
    None
  ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# SP - HIDR - water sources D #####################################################################################################

def convert_data_hidr_water_sources_D(data):

    data_list = []

    for obj in data:
        
        Regional_Directorate = obj["Regional Directorate"]
        Concessions_Management = obj["Concessions Management"]
        Operation_Unit = obj["Operation Unit"]
        Activity = obj["Activity"]
        Year = obj["Year"]
        Month = obj["Month"]
        Day = obj["Day"]
        Month_name = obj["Month_name"]
        volume_of_purchased_seawater = obj["Volume of purchased seawater (D)"]
        volume_of_purchased_groundwater = obj["Volume of purchased groundwater (D)"]
        volume_of_purchased_surface_water = obj["Volume of purchased surface water (D)"]
        volume_of_purchased_supplied_water_total = obj["Volume of purchased supplied water (Total)"]
        volume_of_own_supplied_water_from_treatment_plant = obj["Volume of own supplied water obtained from treatment plant (D)"]
        volum_of_own_supplied_groundwater = obj["Volume of own supplied groundwater (D)"]
        volume_of_own_supplied_surface_water = obj["Volume of own supplied surface water (D)"]
        volume_of_supplied_water_from_own_sources_total = obj["Volume of supplied water obtained from own sources (Total)"]
        
        date = f"{Year}-{Month:02d}-{Day:02d}T00:00:00Z"

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:WaterSourcesDaily:SP-HIDR-waterSources-"+Operation_Unit,
            "type": "WaterSourcesDaily",
            "dataProvider": "Hidralia",
            "regionalDirectorate": Regional_Directorate,
            "concessionsManagement": Concessions_Management,
            "activity": Activity,
            "monthName": Month_name,
            "dateCreated": date,
            "volumeOfPurchasedSeawater": string_to_float(volume_of_purchased_seawater),
            "volumeOfPurchasedGroundwater": string_to_float(volume_of_purchased_groundwater),
            "volumeOfPurchasedSurfaceWater": string_to_float(volume_of_purchased_surface_water),
            "volumeOfPurchasedSuppliedWaterTotal": string_to_float(volume_of_purchased_supplied_water_total),
            "volumeOfOwnSuppliedWaterFromTreatmentPlant": string_to_float(volume_of_own_supplied_water_from_treatment_plant),
            "volumOfOwnSuppliedGroundwater": string_to_float(volum_of_own_supplied_groundwater),
            "volumeOfOwnSuppliedSurfaceWater": string_to_float(volume_of_own_supplied_surface_water),
            "volumeOfSuppliedWaterFromOwnSourcesTotal": string_to_float(volume_of_supplied_water_from_own_sources_total),
            "@context": [
                    None
                ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list


# SP - HIDR - water sources M #####################################################################################################

def convert_data_hidr_water_sources_M(data):

    data_list = []

    for obj in data:
        
        Regional_Directorate = obj["Regional Directorate"]
        Concessions_Management = obj["Concessions Management"]
        Operation_Unit = obj["Operation Unit"]
        Activity = obj["Activity"]
        Year = obj["Year"]
        Month = obj["Month"]
        Month_name = obj["Month_name"]
        volume_of_purchased_seawater = obj["Volumen de agua comprada origen marino (M)"]
        volume_of_purchased_groundwater = obj["Volumen de agua comprada subterránea (M)"]
        volume_of_purchased_surface_water = obj["Volumen de agua comprada superficial (M)"]
        volume_of_own_supplied_water_from_treatment_plant = obj["Volumen de agua propia suministrada obtenida de potabilizadora (M)"]
        volum_of_own_supplied_groundwater = obj["Volumen de agua propia suministrada subterránea (M)"]
        volume_of_own_supplied_surface_water = obj["Volumen de agua propia suministrada superficial (M)"]
        volume_of_supplied_water_from_own_sources_total = obj["Volumen de agua suministrada obtenida de fuentes propias (M)"]
        
        last_day = calendar.monthrange(Year, Month)[1]
        date = f"{Year}-{Month:02d}-{last_day:02d}T00:00:00Z"

        #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:WaterSourcesMonthly:SP-HIDR-waterSources-"+Operation_Unit,
            "type": "WaterSourcesMonthly",
            "dataProvider": "Hidralia",
            "regionalDirectorate": Regional_Directorate,
            "concessionsManagement": Concessions_Management,
            "activity": Activity,
            "monthName": Month_name,
            "dateCreated": date,
            "volumeOfPurchasedSeawater": string_to_float(volume_of_purchased_seawater),
            "volumeOfPurchasedGroundwater": string_to_float(volume_of_purchased_groundwater),
            "volumeOfPurchasedSurfaceWater": string_to_float(volume_of_purchased_surface_water),
            "volumeOfOwnSuppliedWaterFromTreatmentPlant": string_to_float(volume_of_own_supplied_water_from_treatment_plant),
            "volumOfOwnSuppliedGroundwater": string_to_float(volum_of_own_supplied_groundwater),
            "volumeOfOwnSuppliedSurfaceWater": string_to_float(volume_of_own_supplied_surface_water),
            "volumeOfSuppliedWaterFromOwnSourcesTotal": string_to_float(volume_of_supplied_water_from_own_sources_total),
            "@context": [
                    None
                ]
        }
                           

        data_list.append(converted_data)

        

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list

# SP - HIDR - supplied water #####################################################################################################

def convert_data_hidr_supplied_water(data):
    data_list = []

    for obj in data:
        suppliedOwnWater = obj["SuppliedW_o"] #m3
        suppliedExternalWater = obj["SuppliedW_e"] #m3
        registeredWater = obj["RegisteredW"] #m3
        volStoredWater = obj["Volume_reservoir"] #hm3
        precipitationReservoir = obj["Precipitation_reservoir"] #mm
        piezometricLevels = obj["PiezometricL"] #m
        SPI = obj["SPI"] #standard index

     #convert data to ngsi-ld
        converted_data = {
            "id": "urn:ngsi-ld:Leaks:SP-HIDR-suppliedWater-",
            "type": "SuppliedWater",
            "dataProvider": "Hidralia",
            "suppliedOwnWater": string_to_float(suppliedOwnWater),
            "suppliedExternalWater": string_to_float(suppliedExternalWater),
            "registeredWater": string_to_float(registeredWater),
            "volStoredWater": string_to_float(volStoredWater),
            "precipitationReservoir": string_to_float(precipitationReservoir),
            "piezometricLevels": string_to_float(piezometricLevels),
            "SPI": SPI,
            "@context": [
                None
            ]
        }                           

        data_list.append(converted_data)

    if 'invalid' in data:
        raise ValueError('Invalid data')

    return data_list

############################################################################################################################
############################################################################################################################

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)  #remove debugger in final version!!!