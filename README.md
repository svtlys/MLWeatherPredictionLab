# MLWeatherPrediction

# ML Weather Prediction Data Pipeline
# Created by Alyssa Gomez and Angela Wei

## Project Overview

This project builds a simple data pipeline that collects historical weather data from the Open-Meteo API, processes the data using Python, and loads it into a Snowflake database. The dataset will later be used for weather forecasting and automation using Airflow.

The pipeline collects the last 90 days of weather data for:

* Northridge, CA
* Pasadena, CA

The collected data includes:

* Maximum daily temperature
* Mean daily temperature
* Daily precipitation

## Pipeline Architecture

Open-Meteo API
↓
Python Data Extraction (`get_weather_data.py`)
↓
Data Transformation with Pandas
↓
CSV Dataset (`weather_data.csv`)
↓
Snowflake Database (`WEATHER_DATA_LAB1`)

## Project Structure

```
MLWeatherPredictionLab
│
├── src
│   ├── get_weather_data.py
│   └── load_weather_to_snowflake.py
│
├── sql
│   └── lab1.sql
│
├── data
├── docs
├── screenshots
│
├── .gitignore
└── README.md
```

## Setup Instructions

1. Install required Python libraries:

```
pip install requests pandas snowflake-connector-python python-dotenv
```

2. Create a `.env` file in the project root with Snowflake credentials:

```
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
```

3. Run the data extraction script:

```
python src/get_weather_data.py
```

4. Load the data into Snowflake:

```
python src/load_weather_to_snowflake.py
```

## Snowflake Table

Database: `USER_DB_BULLFROG`
Schema: `RAW`
Table: `WEATHER_DATA_LAB1`

## Next Steps

The data stored in Snowflake will be used by the forecasting model and automated using an Airflow DAG to create a complete machine learning data pipeline.
