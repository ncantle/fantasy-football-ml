dome_stadiums = {
    "Lucas Oil Stadium",
    "Mercedes-Benz Stadium",
    "Caesars Superdome",
    "NRG Stadium",
    "Allegiant Stadium",
    "AT&T Stadium",
    "State Farm Stadium",
    "U.S. Bank Stadium",
    "Ford Field"
}

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import requests

# Load env & connect
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)
API_KEY = os.getenv('VISUAL_CROSSING_API_KEY')

# Stadium → Location mapping
stadiums = {
    "Allegiant Stadium": "Las Vegas, NV",
    "AT&T Stadium": "Arlington, TX",
    "Bank of America Stadium": "Charlotte, NC",
    "Caesars Superdome": "New Orleans, LA",
    "FedExField": "Landover, MD",
    "Ford Field": "Detroit, MI",
    "Gillette Stadium": "Foxborough, MA",
    "Hard Rock Stadium": "Miami Gardens, FL",
    "Highmark Stadium": "Orchard Park, NY",
    "Lambeau Field": "Green Bay, WI",
    "Levi's Stadium": "Santa Clara, CA",
    "Lincoln Financial Field": "Philadelphia, PA",
    "Lucas Oil Stadium": "Indianapolis, IN",
    "Lumen Field": "Seattle, WA",
    "Mercedes-Benz Stadium": "Atlanta, GA",
    "MetLife Stadium": "East Rutherford, NJ",
    "M&T Bank Stadium": "Baltimore, MD",
    "NRG Stadium": "Houston, TX",
    "Paycor Stadium": "Cincinnati, OH",
    "Raymond James Stadium": "Tampa, FL",
    "SoFi Stadium": "Inglewood, CA",
    "Soldier Field": "Chicago, IL",
    "State Farm Stadium": "Glendale, AZ",
    "TIAA Bank Field": "Jacksonville, FL",
    "U.S. Bank Stadium": "Minneapolis, MN",
    "Arrowhead Stadium": "Kansas City, MO",
    "Acrisure Stadium": "Pittsburgh, PA",
    "Empower Field at Mile High": "Denver, CO",
    "FirstEnergy Stadium": "Cleveland, OH"  # Cleveland Browns
}

dome_stadiums = {
    "Lucas Oil Stadium",
    "Mercedes-Benz Stadium",
    "Caesars Superdome",
    "NRG Stadium",
    "Allegiant Stadium",
    "AT&T Stadium",
    "State Farm Stadium",
    "U.S. Bank Stadium",
    "Ford Field",
    "SoFi Stadium",
    "Lumen Field"
}

# Pull games from DB
schedule_df = pd.read_sql("SELECT season, week, stadium, game_date FROM games", engine)

# Fetch weather for each game
weather_records = []

BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

for _, game in schedule_df.iterrows():
    stadium = game['stadium']
    location = stadiums.get(stadium)
    if not location:
        continue

    params = {
        "unitGroup": "us",
        "key": API_KEY,
        "include": "days",
        "elements": "datetime,temp,precip,windspeed",
        "contentType": "json"
    }

    response = requests.get(f"{BASE_URL}/{location}/{game['game_date']}", params=params)
    data = response.json()
    day = data["days"][0]

    weather_records.append({
        "season": game["season"],
        "week": game["week"],
        "stadium": stadium,
        "temperature": day.get("temp"),
        "precipitation": day.get("precip"),
        "wind_speed": day.get("windspeed"),
        "dome": stadium in dome_stadiums
    })

# Ingest into DB
weather_df = pd.DataFrame(weather_records)
weather_df.drop_duplicates().to_sql('weather', engine, if_exists='replace', index=False)
print("✅ Weather data ingested into PostgreSQL.")
