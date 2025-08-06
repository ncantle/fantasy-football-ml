import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def add_home_away_rolling_and_std_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds rolling and season-to-date averages split by home/away games.
    Relies on an existing `home_game` column in df.
    """
    print('Adding home/away rolling and std averages...')
    df = df.copy()
    df["week"] = df["week"].astype(int)
    df = df.sort_values(by=['player_id', 'season', 'week'])

    stats = [
        'fantasy_points', 'targets', 'carries',
        'passing_yards', 'rushing_yards', 'receiving_yards',
        'passing_tds', 'rushing_tds', 'receiving_tds'
    ]
    rolling_windows = [3, 5]

    for side, flag in [('home', True), ('away', False)]:
        side_df = df[df['home_game'] == flag].copy()
        side_df = side_df.sort_values(by=['player_id', 'season', 'week'])

        # Rolling averages
        for window in rolling_windows:
            for stat in stats:
                col_name = f'{stat}_{side}_avg_{window}wk'
                side_df[col_name] = (
                    side_df
                    .groupby(['player_id', 'season'])[stat]
                    .transform(lambda x: x.shift(1).rolling(window=window, min_periods=window).mean())
                )

        # Season-to-date averages
        for stat in stats:
            col_name = f'{stat}_{side}_std_avg'
            side_df[col_name] = (
                side_df
                .groupby(['player_id', 'season'])[stat]
                .transform(lambda x: x.shift(1).expanding().mean())
            )

        # Merge newly created columns back to main df
        merge_cols = ['season', 'week', 'player_id']
        added_cols = [col for col in side_df.columns if col not in df.columns and col not in merge_cols]
        df = df.merge(side_df[merge_cols + added_cols], on=merge_cols, how='left')

    return df