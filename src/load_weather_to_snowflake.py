import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

# Load CSV file
df = pd.read_csv('data/weather_data.csv')

#Connecting to Snowflake
conn = snowflake.connector.connect(
    user = os.getenv('SNOWFLAKE_USER'),
    password = os.getenv('SNOWFLAKE_PASSWORD'),
    account = os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
    database = os.getenv('SNOWFLAKE_DATABASE'),
    schema = os.getenv('SNOWFLAKE_SCHEMA') 
)
cursor = conn.cursor()

print('Connected to Snowflake!')

# Clear existing data to avoid duplicates
cursor.execute("DELETE FROM WEATHER_DATA_LAB1")


#Insertion Process
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO WEATHER_DATA_LAB1
        (CITY, DATE, TEMP_MAX, TEMP_MEAN, PRECIPITATION)
        VALUES (%s,%s,%s,%s,%s)
    """, (
        row["city"],
        row["date"],
        row["temp_max"],
        row["temp_mean"],
        row["precipitation"]
    ))

conn.commit()

cursor.close()
conn.close()

print("Weather data successfully loaded into Snowflake")