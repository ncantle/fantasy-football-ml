import pandas as pd
import itertools
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)

# Example 32 teams (abbreviations)
teams = ['ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE', 'DAL', 'DEN', 'DET', 'GB', 
         'HOU', 'IND', 'JAX', 'KC', 'LV', 'LAC', 'LAR', 'MIA', 'MIN', 'NE', 'NO', 'NYG', 
         'NYJ', 'PHI', 'PIT', 'SEA', 'SF', 'TB', 'TEN', 'WAS']

# Randomly generate pairings for 18 weeks
weeks = list(range(1, 19))
schedule = []

# Just for mock purposes: each team plays a different one at home each week
for week in weeks:
    for i, home_team in enumerate(teams[:16]):  # Limit to 16 matchups
        away_team = teams[(i + week) % 32]  # Rotate for variation
        schedule.append({
            'season': 2024,
            'week': week,
            'home_team': home_team,
            'away_team': away_team
        })

df = pd.DataFrame(schedule)
teams_df = pd.read_sql('SELECT team_id, abbreviation FROM teams', engine)

df = df.merge(teams_df, how='left', left_on='home_team', right_on='abbreviation')
df.rename(columns = {'team_id':'home_team_id'}, inplace=True)
df.drop(columns = ['abbreviation'], inplace = True)

df = df.merge(teams_df, how = 'left', left_on = 'away_team', right_on = 'abbreviation')
df.rename(columns = {'team_id':'away_team_id'}, inplace=True)
df.drop(columns = ['abbreviation'], inplace = True)

df.to_csv('data/raw/nfl_schedule.csv', index=False)

df.to_sql('games', engine, if_exists='replace', index=False)
print("✅ Games ingested into PostgreSQL.")