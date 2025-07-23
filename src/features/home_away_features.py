import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def add_home_away_rolling_and_std_averages(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """
    Adds rolling averages for fantasy_points, targets, and carries separated by home and away games.
    """
    # Load credentials and connect to DB
    load_dotenv()
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)

    # Load games and teams from Postgres
    games_df = pd.read_sql("SELECT season, week, home_team, away_team FROM games", engine)
    teams_df = pd.read_sql("SELECT team_id, abbreviation FROM teams", engine)

    # Add home/away team IDs
    games_df = games_df.merge(teams_df, how='left', left_on='home_team', right_on='abbreviation')\
                       .rename(columns={'team_id': 'home_team_id'})\
                       .drop(columns='abbreviation')
    games_df = games_df.merge(teams_df, how='left', left_on='away_team', right_on='abbreviation')\
                       .rename(columns={'team_id': 'away_team_id'})\
                       .drop(columns='abbreviation')

    # Merge into main df
    df = df.merge(games_df[['season', 'week', 'home_team_id', 'away_team_id']], on=['season', 'week'], how='left')

    # Tag home/away
    df['home_away'] = np.where(df['team_id'] == df['home_team_id'], 'home', 'away')

    df = df.sort_values(by=['player_id', 'season', 'week'])

    # Initialize list to collect results
    results = []

    # For each home/away type, compute rolling stats
    for side in ['home', 'away']:
        side_df = df[df['home_away'] == side].copy()

        # We'll track keys for merge later
        keys = ['season', 'week', 'player_id']

        for stat in ['fantasy_points', 'targets', 'carries']:
            col_name = f'{stat}_{side}_avg'

            # Calculate the rolling average and align indexes
            side_df[col_name] = (
                side_df
                .groupby('player_id')[stat]
                .transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).mean())
            )

        results.append(side_df[keys + [f'{s}_{side}_avg' for s in ['fantasy_points', 'targets', 'carries']]])

    # Merge the rolling stats back in
    for result in results:
        df = df.merge(result, on=['season', 'week', 'player_id'], how='left')

        # Compute season-to-date home/away averages
    for side in ['home', 'away']:
        side_df = df[df['home_away'] == side].copy()
        side_df = side_df.sort_values(by=['player_id', 'season', 'week'])

        for stat in ['fantasy_points', 'targets', 'carries']:
            col_name = f'{stat}_{side}_std_avg'

            # Cumulative mean excluding current row
            side_df[col_name] = (
                side_df
                .groupby(['player_id', 'season'])[stat]
                .transform(lambda x: x.shift(1).expanding().mean())
            )

        # Merge back into main df
        std_cols = ['season', 'week', 'player_id'] + [f'{s}_{side}_std_avg' for s in ['fantasy_points', 'targets', 'carries']]
        df = df.merge(side_df[std_cols], on=['season', 'week', 'player_id'], how='left')

    return df
