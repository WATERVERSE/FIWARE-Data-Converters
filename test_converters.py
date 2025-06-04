import json
from main import convert_data_pwn_hwl, convert_data_iot_sensor, convert_data_riwa_rijn, convert_data_pa_water_sensors, convert_data_pa_wind_sensors

HWL_test_data = [
    {
        "id": "7994515Totaalchlorofyl",
        "Analysedatum": "2022-12-23T09:50:04",
        "Monsterdatum": "2022-12-22T13:33:00",
        "Waarde": 18.87,
        "Analyse": "CHL-FLUO",
        "Component": "Totaal chlorofyl",
        "Eenheid": "µg/l",
        "Resultaat": 19,
        "Plaats": "Andijk",
        "Sublocatie": "IJsselmeer",
        "Monsterpunt": "Innamepunt IJsselmeer"
    },
    {
        "id": "7096509nitriet",
        "Analysedatum": "2020-08-04T08:00:14",
        "Monsterdatum": "2020-08-03T11:49:00",
        "Waarde": 0.00447,
        "Analyse": "NO2",
        "Component": "Nitriet",
        "Eenheid": "mg/l N",
        "Resultaat": 0.004,
        "Plaats": "Andijk",
        "Sublocatie": "IJsselmeer",
        "Monsterpunt": "Innamepunt IJsselmeer"
    },
    {
        "id": "7778481n-nitrosodimethylamine(NDMA)",
        "Analysedatum": "2022-05-30T10:48:39",
        "Monsterdatum": "2022-04-26T09:55:00",
        "Waarde": 0,
        "Analyse": "NDMA",
        "Component": "n-nitrosodimethylamine (NDMA)",
        "Eenheid": "ng/l",
        "Resultaat": "<2.0",
        "Plaats": "Andijk",
        "Sublocatie": "IJsselmeer",
        "Monsterpunt": "Innamepunt IJsselmeer"
    },
    {
        "id": "7254148nitraat",
        "Analysedatum": "2020-12-28T16:30:04",
        "Monsterdatum": "2020-12-28T12:47:00",
        "Waarde": 0.75753,
        "Analyse": "NO3",
        "Component": "Nitraat",
        "Eenheid": "mg/l N",
        "Resultaat": 0.76,
        "Plaats": "Andijk",
        "Sublocatie": "IJsselmeer",
        "Monsterpunt": "Innamepunt IJsselmeer"
    },
    {
        "id": "7339776calcium",
        "Analysedatum": "2021-03-09T15:31:08",
        "Monsterdatum": "2021-03-08T12:55:00",
        "Waarde": 64.515,
        "Analyse": "CA",
        "Component": "Calcium",
        "Eenheid": "mg/l Ca",
        "Resultaat": 64.52,
        "Plaats": "Andijk",
        "Sublocatie": "IJsselmeer",
        "Monsterpunt": "Innamepunt IJsselmeer"
    }
]

IoT_test_data = [
    {
        "position":{
        "value":1,
        "context":{
            "lat":52.6941,
            "lng":5.3147
        }
        },
        "conductivity":600.53,
        "course":310.20,
        "speed":2
    },
    {
        "position":{
        "value":1,
        "context":{
            "lat":62.6941,
            "lng":6.3147
        }
        },
        "conductivity":700.53,
        "course":410.20,
        "speed":3
    }
]

Riwa_Rijn_test_data = [
    {
        "rp": "LOB",
        "omschrijving": "Lobith (R863/R)",
        "x-coördinaat": 203500.0,
        "y-coördinaat": 429750.0,
        "par": 8632,
        "naam": "aminomethylfosfonzuur (AMPA)",
        "Engelse naam": "aminomethylphosphonic acid (AMPA)",
        "cas-nummer": "1066-51-9",
        "datum": "2023-01-04",
        "teken": "<",
        "waarde": 0.2,
        "dimensie": "µg/l"
    },
    {
        "rp": "LOB",
        "omschrijving": "Lobith (R863/R)",
        "x-coördinaat": 203500.0,
        "y-coördinaat": 429750.0,
        "par": 230,
        "naam": "chloride",
        "Engelse naam": "chloride",
        "cas-nummer": "16887-00-6",
        "datum": "2023-01-04",
        "teken": "+",
        "waarde": 69.0,
        "dimensie": "mg/l"
    },
    {
        "rp": "LOB",
        "omschrijving": "Lobith (R863/R)",
        "x-coördinaat": 203500.0,
        "y-coördinaat": 429750.0,
        "par": 6380,
        "naam": "valsartan",
        "Engelse naam": "valsartan",
        "cas-nummer": "137862-53-4",
        "datum": "2023-01-04",
        "teken": "+",
        "waarde": 0.099,
        "dimensie": "µg/l"
    }
]

PA_water_test_data = [
    {
        "_id": 3,
        "NAMED_ID": "AWWYIB_QT4053.211802025-03-05 00:31:03",
        "NAME": "AWWYIB_QT40",
        "IP_TREND_VALUE": 53.2118,
        "IP_TREND_TIME": "2025-03-05T00:31:03",
        "IP_INPUT_QUALITY": "Goed",
        "IP_DESCRIPTION": "EGV Inlaat",
        "IP_ENG_UNITS": "mS/m"
    },
    {
        "_id": 1,
        "NAMED_ID": "ANWYAA_QT1054.108802025-03-04 11:22:15",
        "NAME": "ANWYAA_QT10",
        "IP_TREND_VALUE": 54.1088,
        "IP_TREND_TIME": "2025-03-04T11:22:15",
        "IP_INPUT_QUALITY": "Goed",
        "IP_DESCRIPTION": "Geleidbaarheidsmt inlaatkanaal 1",
        "IP_ENG_UNITS": "mS/m"
    },
    {
        "_id": 7,
        "NAMED_ID": "ANWYAA_QT2054.456002025-03-05 06:07:16",
        "NAME": "ANWYAA_QT20",
        "IP_TREND_VALUE": 54.456,
        "IP_TREND_TIME": "2025-03-05T06:07:16",
        "IP_INPUT_QUALITY": "Goed",
        "IP_DESCRIPTION": "Geleidbaarheidsmt inlaatkanaal 2",
        "IP_ENG_UNITS": "mS/m"
    }
]

PA_wind_test_data = [
    {
        "_id": 18,
        "NAMED_ID": "AWA0XT_GT10203.060002025-03-04 15:52:10",
        "NAME": "AWA0XT_GT10",
        "IP_TREND_VALUE": 203.06,
        "IP_TREND_TIME": "2025-03-04T15:52:10",
        "IP_INPUT_QUALITY": "Goed",
        "IP_DESCRIPTION": "Windrichting",
        "IP_ENG_UNITS": "°"
    },
    {
        "_id": 6,
        "NAMED_ID": "AWA0XT_ST104.355472025-03-04 12:37:10",
        "NAME": "AWA0XT_ST10",
        "IP_TREND_VALUE": 4.35547,
        "IP_TREND_TIME": "2025-03-04T12:37:10",
        "IP_INPUT_QUALITY": "Goed",
        "IP_DESCRIPTION": "Windsnelheid",
        "IP_ENG_UNITS": "m/s"
    },
    {
        "_id": 4,
        "NAMED_ID": "AWA0XT_WT105.850692025-03-04 14:22:10",
        "NAME": "AWA0XT_WT10",
        "IP_TREND_VALUE": 5.85069,
        "IP_TREND_TIME": "2025-03-04T14:22:10",
        "IP_INPUT_QUALITY": "Goed",
        "IP_DESCRIPTION": "Windstoten",
        "IP_ENG_UNITS": "m/s"
    }
]


#result = convert_data_pwn_hwl(HWL_test_data)
#result = convert_data_iot_sensor(IoT_test_data)
#result = convert_data_riwa_rijn(Riwa_Rijn_test_data)
#result = convert_data_pa_water_sensors(PA_water_test_data)
result = convert_data_pa_wind_sensors(PA_wind_test_data)

print("TEST OUTPUT: ", json.dumps(result, indent=4, ensure_ascii=False))
