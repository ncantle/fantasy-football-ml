import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def fetch_sleeper_injuries(engine):
    # Fetch injuries from Sleeper API (this endpoint might change; confirm from their docs)
    response = requests.get("https://api.sleeper.app/v1/players/nfl")
    players_data = response.json()

    players_df = pd.DataFrame.from_dict(players_data, orient='index')
    injury_df = players_df[['full_name', 'injury_status']].copy()
    injury_df = injury_df.rename(columns={'full_name': 'name'})
    injury_df = injury_df[injury_df['injury_status'].notnull()]

    # Map player_id from players table
    players_query = pd.read_sql('SELECT player_id, name FROM players', engine)
    injury_df = injury_df.merge(players_query, how='left', on='name')

    # Add season & week (manually or from schedule)
    injury_df['season'] = 2024  # adjust as needed
    injury_df['week'] = 1       # adjust as needed

    # Select and rename columns for PostgreSQL
    final_df = injury_df[['player_id', 'season', 'week', 'injury_status']].copy()
    final_df = final_df.dropna(subset=['player_id'])

    # Save to PostgreSQL
    final_df.drop_duplicates().to_sql('injuries', engine, if_exists='replace', index=False)
    print("Injuries ingested into PostgreSQL.")
