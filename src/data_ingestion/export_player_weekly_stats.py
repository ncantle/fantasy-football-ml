import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load credentials
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)

# Load NFLFastR player weekly stats parquet file
df = pd.read_parquet("data/raw/nflfastr/player_stats_2023.parquet")

# Prepare weekly stats dataframe
weekly_stats_df = df.rename(columns={
    'season': 'season',
    'week': 'week',
    'player_id': 'player_id',
    'recent_team': 'team_abbreviation',
    'targets': 'targets',
    'carries': 'carries',
    'offense_snaps': 'snaps'
})

# Link team_id from teams table
teams_df = pd.read_sql('SELECT team_id, abbreviation FROM teams', engine)
weekly_stats_df = weekly_stats_df.merge(teams_df, how='left', left_on='team_abbreviation', right_on='abbreviation')

# Final columns for ingestion
weekly_stats_df = weekly_stats_df[['season', 'week', 'player_id', 'team_id', 'fantasy_points', 'targets', 'carries']]

# Drop rows where player_id or team_id is missing (data integrity)
weekly_stats_df = weekly_stats_df.dropna(subset=['player_id', 'team_id'])

# Ensure types match schema
weekly_stats_df['player_id'] = weekly_stats_df['player_id'].str.replace(r'[^a-zA-Z0-9]', '', regex=True)
weekly_stats_df['player_id'] = weekly_stats_df['player_id'].astype(int)
weekly_stats_df['team_id'] = weekly_stats_df['team_id'].astype(int)

# Ingest into PostgreSQL
weekly_stats_df.to_sql('weekly_stats', engine, if_exists='replace', index=False)

print("✅ Weekly stats successfully ingested into PostgreSQL.")

# SQL Query to assemble player-week stats
query = """
SELECT
    ws.season,
    ws.week,
    p.player_id,
    p.name,
    t.abbreviation AS team,
    p.team_id,
    p.position,
    CASE 
        WHEN p.team_id = g.home_team_id THEN g.away_team
        ELSE g.home_team
    END AS opponent,
    ws.fantasy_points,
    ws.targets,
    ws.carries,
    d.depth_position,
    i.injury_status
FROM weekly_stats ws
LEFT JOIN players p ON ws.player_id = p.player_id
LEFT JOIN teams t ON p.team_id = t.team_id
LEFT JOIN games g ON ws.season = g.season AND ws.week = g.week
LEFT JOIN depth_chart d ON p.player_id = d.player_id
LEFT JOIN injuries i 
    ON p.player_id = i.player_id 
    AND ws.week = i.week 
    AND ws.season = i.season
ORDER BY ws.season, ws.week, p.name;
"""

# Execute query and export
df = pd.read_sql(query, engine).drop_duplicates()
df.to_csv("data/processed/player_weekly_stats.csv", index=False)

print("✅ Exported to data/processed/player_weekly_stats.csv")
