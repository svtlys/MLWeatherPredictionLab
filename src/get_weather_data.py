import requests 
import pandas as pd
from datetime import datetime, timedelta

#Open-Meteo API Endpoint
URL = 'https://archive-api.open-meteo.com/v1/archive'

#Cities being used

cities = {
    'Northridge' : (34.2381, -118.5301),
    'Pasadena': (34.1478, -118.1445)
}

# Get last 90 days
end_date = datetime.today()
start_date = end_date - timedelta(days=90)

start_date = start_date.strftime("%Y-%m-%d")
end_date = end_date.strftime("%Y-%m-%d")

all_data = []

for city, coords in cities.items():

    params = {
        "latitude": coords[0],
        "longitude": coords[1],
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_mean,precipitation_sum",
        "timezone": "auto"
    }

    response = requests.get(URL, params=params)
    data = response.json()

    daily = data["daily"]

    df = pd.DataFrame({
        "date": daily["time"],
        "temp_max": daily["temperature_2m_max"],
        "temp_mean": daily["temperature_2m_mean"],
        "precipitation": daily["precipitation_sum"]
    })

    df["city"] = city

    all_data.append(df) 

# Combine both cities
weather_df = pd.concat(all_data)

# Print preview
print(weather_df.head())

# Saving dataset
weather_df.to_csv("data/weather_data.csv", index=False)

print("Weather data saved to data/weather_data.csv")