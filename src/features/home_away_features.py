import pandas as pd

def add_home_away_rolling_averages(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """
    Adds rolling averages for fantasy_points, targets, and carries separated by home and away games.
    
    Parameters:
        df: DataFrame with at least columns ['player_id', 'season', 'week', 'home_away', 'fantasy_points', 'targets', 'carries']
        window: Number of past games to average (default 3)
    
    Returns:
        DataFrame with new columns:
            fantasy_points_home_avg, targets_home_avg, carries_home_avg,
            fantasy_points_away_avg, targets_away_avg, carries_away_avg
    """
    df = df.sort_values(by=['player_id', 'season', 'week'])
    df['home_away'] = df.apply(lambda row: 'home' if row['team_id'] == row['home_team_id'] else 'away', axis=1)

    
    # Initialize new columns
    for stat in ['fantasy_points', 'targets', 'carries']:
        df[f'{stat}_home_avg'] = None
        df[f'{stat}_away_avg'] = None
    
    # Compute rolling averages for home games
    home_mask = df['home_away'] == 'home'
    df.loc[home_mask, 'fantasy_points_home_avg'] = (
        df[home_mask].groupby('player_id')['fantasy_points']
        .apply(lambda x: x.shift(1).rolling(window=window, min_periods=1).mean())
    )
    df.loc[home_mask, 'targets_home_avg'] = (
        df[home_mask].groupby('player_id')['targets']
        .apply(lambda x: x.shift(1).rolling(window=window, min_periods=1).mean())
    )
    df.loc[home_mask, 'carries_home_avg'] = (
        df[home_mask].groupby('player_id')['carries']
        .apply(lambda x: x.shift(1).rolling(window=window, min_periods=1).mean())
    )
    
    # Compute rolling averages for away games
    away_mask = df['home_away'] == 'away'
    df.loc[away_mask, 'fantasy_points_away_avg'] = (
        df[away_mask].groupby('player_id')['fantasy_points']
        .apply(lambda x: x.shift(1).rolling(window=window, min_periods=1).mean())
    )
    df.loc[away_mask, 'targets_away_avg'] = (
        df[away_mask].groupby('player_id')['targets']
        .apply(lambda x: x.shift(1).rolling(window=window, min_periods=1).mean())
    )
    df.loc[away_mask, 'carries_away_avg'] = (
        df[away_mask].groupby('player_id')['carries']
        .apply(lambda x: x.shift(1).rolling(window=window, min_periods=1).mean())
    )
    
    return df
