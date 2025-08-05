import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def fetch_sleeper_players(engine):
    # --- Fetch Teams ---
    teams_data = [
        {"name": "Arizona Cardinals", "abbreviation": "ARI"},
        {"name": "Atlanta Falcons", "abbreviation": "ATL"},
        {"name": "Baltimore Ravens", "abbreviation": "BAL"},
        {"name": "Buffalo Bills", "abbreviation": "BUF"},
        {"name": "Carolina Panthers", "abbreviation": "CAR"},
        {"name": "Chicago Bears", "abbreviation": "CHI"},
        {"name": "Cincinnati Bengals", "abbreviation": "CIN"},
        {"name": "Cleveland Browns", "abbreviation": "CLE"},
        {"name": "Dallas Cowboys", "abbreviation": "DAL"},
        {"name": "Denver Broncos", "abbreviation": "DEN"},
        {"name": "Detroit Lions", "abbreviation": "DET"},
        {"name": "Green Bay Packers", "abbreviation": "GB"},
        {"name": "Houston Texans", "abbreviation": "HOU"},
        {"name": "Indianapolis Colts", "abbreviation": "IND"},
        {"name": "Jacksonville Jaguars", "abbreviation": "JAX"},
        {"name": "Kansas City Chiefs", "abbreviation": "KC"},
        {"name": "Las Vegas Raiders", "abbreviation": "LV"},
        {"name": "Los Angeles Chargers", "abbreviation": "LAC"},
        {"name": "Los Angeles Rams", "abbreviation": "LAR"},
        {"name": "Miami Dolphins", "abbreviation": "MIA"},
        {"name": "Minnesota Vikings", "abbreviation": "MIN"},
        {"name": "New England Patriots", "abbreviation": "NE"},
        {"name": "New Orleans Saints", "abbreviation": "NO"},
        {"name": "New York Giants", "abbreviation": "NYG"},
        {"name": "New York Jets", "abbreviation": "NYJ"},
        {"name": "Philadelphia Eagles", "abbreviation": "PHI"},
        {"name": "Pittsburgh Steelers", "abbreviation": "PIT"},
        {"name": "San Francisco 49ers", "abbreviation": "SF"},
        {"name": "Seattle Seahawks", "abbreviation": "SEA"},
        {"name": "Tampa Bay Buccaneers", "abbreviation": "TB"},
        {"name": "Tennessee Titans", "abbreviation": "TEN"},
        {"name": "Washington Commanders", "abbreviation": "WAS"}
    ]
    teams_df = pd.DataFrame(teams_data).drop_duplicates().reset_index().rename(columns = {'index':'team_id'})
    teams_df.to_sql('teams', engine, if_exists='append', index=False)
    print("Teams ingested into PostgreSQL.")

    # --- Fetch Players ---
    print("Fetching players from Sleeper API...")
    response = requests.get('https://api.sleeper.app/v1/players/nfl')
    players_data = response.json()

    df = pd.DataFrame.from_dict(players_data, orient='index')

    df = df[['full_name', 'position', 'team', 'birth_date']]
    df = df.rename(columns={
        'full_name': 'name',
        'birth_date': 'birthdate'
    })

    # Filter out entries without a name (defensive team slots, placeholders)
    df = df[df['name'].notnull() & df['position'].notnull()]

    # Handle team ID mapping via abbreviation
    teams_query = pd.read_sql('SELECT team_id, abbreviation FROM teams', engine)
    df = df.merge(teams_query, how='left', left_on='team', right_on='abbreviation')

    df = df[['name', 'position', 'team_id', 'birthdate']].reset_index().rename(columns = {'index': 'player_id'})

    # Write to Postgres
    df.drop_duplicates().to_sql('players', engine, if_exists='replace', index=False)
    print("Sleeper players ingested into PostgreSQL.")