import pandas as pd

def add_rolling_features(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """
    Adds rolling average features for fantasy points, targets, carries, snaps.
    Expects columns: player_id, season, week, fantasy_points, targets, carries, snaps
    """
    df = df.sort_values(by=['player_id', 'season', 'week'])
    
    rolling_cols = ['fantasy_points', 'targets', 'carries', 'snaps']
    
    for col in rolling_cols:
        rolling_col_name = f'rolling_{col}_avg_{window}'
        df[rolling_col_name] = (
            df.groupby('player_id')[col]
            .shift(1)
            .rolling(window=window, min_periods=1)
            .mean()
            .reset_index(drop=True)
        )
    
    return df
