# Waterverse Data Converters (User Guide)

## Table of Contents

- [Overview](#overview)
- [Functionality](#functionality)
- [Requirements](#requirements)
- [How it works](#how-it-works)
- [Setting up the Docker](#setting-up-the-docker)
- [Future Improvements](#future-improvements)
- [Acknowledgments](#acknowledgments)

## Overview

The Waterverse Data Converters is a Data Harmonization Tool designed to transform input data into **NGSI-LD** format using **FIWARE Smart Data Models (SDMs)**. It works as an API endpoint, accepting POST requests with data to harmonize. The tool supports multiple pilots and diverse unstructured data, and it is fully integrated with WDME pipelines to automate harmonization of large datasets efficiently, improving interoperability and data quality within the WATERVERSE platform.

## Functionality

* Accepts JSON data via POST requests at the API endpoint.
* Identifies the type of input data using fields such as `resource_id`.
* Selects and executes the appropriate Data Converter function to map input fields to the target SDM.
* Returns harmonized data in NGSI-LD JSON format.
* Supports multiple pilots with 44 converters currently implemented, including recent updates for Netherlands, Cyprus, Finland, and Spain.

## Requirements

### Input
* JSON data matching the expected structure for the target data model.
* Send the data via POST requests to the API endpoint.

### Output
* NGSI-LD formatted JSON data, following FIWARE Smart Data Models.

### System Requirements
* Python installed (for running outside Docker).
* Docker for containerized deployment (optional but recommended).
* Network access to the server hosting the API.

## How it works

When a POST request is sent to the API endpoint, the tool first identifies the input data using the `resource_id` field (or other similar identifying fields). Based on this identification, it selects the appropriate Data Converter function. The converter maps the input data fields to the corresponding FIWARE Smart Data Model fields and returns the harmonized data in NGSI-LD JSON format.

## Setting up the docker

1. Build the docker
```bash
sudo docker build -t waterverse .
```

2. Run the docker
```bash
sudo docker run -d -p 8080:8080 waterverse
```

### To make sure the docker is running:

1. List running containers
```bash
sudo docker ps
```

2. Show container logs
```bash
sudo docker logs <container_id>
```

## Future Improvements

Add more Data Converters to support new data models as required by users or the WATERVERSE project.

## Acknowledgments

This project has been funded by the [WATERVERSE project](https://waterverse.eu/) of the European Union’s Horizon Europe programme under Grant Agreement no 101070262.

WATERVERSE is a project that promotes the use of FAIR (Findable, Accessible, Interoperable, and Reusable) data principles to improve water sector data management and sharing.
