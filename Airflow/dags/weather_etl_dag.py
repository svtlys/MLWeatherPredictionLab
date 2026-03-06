from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta
from datetime import datetime
import pandas as pd  
import requests


@task
def fetch_weather_data():
    URL = 'https://archive-api.open-meteo.com/v1/archive'
    cities = {
        'Northridge': (34.2381, -118.5301),
        'Pasadena': (34.1478, -118.1445)
    }

    end_date = datetime.today()
    start_date = end_date - timedelta(days=90)
    
    all_data = []
    for city, coords in cities.items():
        params = {
            "latitude": coords[0],
            "longitude": coords[1],
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "daily": "temperature_2m_max,temperature_2m_mean,precipitation_sum",
            "timezone": "auto"
        }
        response = requests.get(URL, params=params)
        data = response.json()["daily"]
        
        df = pd.DataFrame({
            "date": data["time"],
            "temp_max": data["temperature_2m_max"],
            "temp_mean": data["temperature_2m_mean"],
            "precipitation": data["precipitation_sum"],
            "city": city
        })
        all_data.append(df)

    weather_df = pd.concat(all_data)
    
    return weather_df.to_dict(orient='records')

@task
def load_to_snowflake(data_dict):
    weather_df = pd.DataFrame(data_dict)
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    
    
    print(f"Loading {len(weather_df)} rows to Snowflake")

with DAG(
    dag_id='weather_to_snowflake_pipeline',
    start_date=datetime(2026, 2, 23),
    catchup=False,
    schedule='30 2 * * *',
    tags=['ETL']
) as dag:
    
    weather_json = fetch_weather_data()
    load_to_snowflake(weather_json)