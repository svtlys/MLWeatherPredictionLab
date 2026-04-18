# ML Weather Data Analytics Pipeline  
Created by Alyssa Gomez and Angela Wei  

## Project Overview

This project implements an end-to-end data pipeline that collects historical weather data from the Open-Meteo API, stores it in Snowflake, transforms it using dbt, and prepares it for visualization in Tableau.

The pipeline is automated using Apache Airflow to handle data ingestion and transformation workflows.

---

## Data Description

The pipeline collects the last 90 days of weather data for:

- Northridge, CA  
- Pasadena, CA  

The collected data includes:

- Maximum daily temperature  
- Mean daily temperature  
- Daily precipitation  

---

## Pipeline Architecture

Open-Meteo API  
↓  
Airflow (ETL Pipeline)  
↓  
Snowflake (RAW Table: WEATHER_DATA_LAB1)  
↓  
dbt (ELT Transformations)  
↓  
Snowflake (Analytics Tables: weather_summary, weather_analytics)  
↓  
Tableau Dashboard  

---

## Technologies Used

- **Apache Airflow** – Workflow orchestration (ETL)  
- **Snowflake** – Data warehouse  
- **dbt** – Data transformations (ELT)  
- **Python (Pandas, Requests)** – Data processing  
- **Tableau** – Data visualization  

---

## Features

- Automated data ingestion using Airflow  
- Storage of raw weather data in Snowflake  
- dbt transformations including:
  - Aggregated weather summaries  
  - 7-day moving average temperature  
  - Rolling precipitation calculations  
- End-to-end scheduled data pipeline  

---

## Project Structure
