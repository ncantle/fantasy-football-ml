import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)

response = requests.get('https://api.sleeper.app/v1/players/nfl')
players_data = response.json()
players_df = pd.DataFrame.from_dict(players_data, orient='index')

depth_chart_df = players_df[['full_name', 'position', 'depth_chart_position', 'team']].copy()
depth_chart_df = depth_chart_df.rename(columns={
    'full_name': 'name',
    'depth_chart_position': 'depth_position'
})
depth_chart_df = depth_chart_df[depth_chart_df['name'].notnull() & depth_chart_df['position'].notnull()]

# Link team ID from existing teams table
teams_query = pd.read_sql('SELECT team_id, abbreviation FROM teams', engine)
depth_chart_df = depth_chart_df.merge(teams_query, how='left', left_on='team', right_on='abbreviation')

# Link player ID from existing players table
players_query = pd.read_sql('SELECT player_id, name FROM players', engine)
depth_chart_df = depth_chart_df.merge(players_query, how='left', on='name')

# Drop rows where we are missing player_id or team_id because those are required for our schema
final_df = depth_chart_df[['player_id', 'team_id', 'position', 'depth_position']].copy()

# Drop rows where player_id or team_id is string 'None'
final_df = final_df[(final_df['player_id'] != 'None') & (final_df['team_id'] != 'None')]

# Drop rows where player_id or team_id is NaN
final_df = final_df.dropna(subset=['player_id', 'team_id'])

# Ensure player_id and team_id are integers
final_df['player_id'] = final_df['player_id'].astype(int)
final_df['team_id'] = final_df['team_id'].astype(int)

# Clean depth_position
final_df['depth_position'] = final_df['depth_position'].replace('None', pd.NA)
final_df['depth_position'] = final_df['depth_position'].fillna(0)
final_df['depth_position'] = final_df['depth_position'].astype(str)

final_df.drop_duplicates().to_sql('depth_chart', engine, if_exists='replace', index=False)
print("✅ Depth chart ingested into PostgreSQL.")
