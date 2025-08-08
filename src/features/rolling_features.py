import pandas as pd

def add_season_to_date_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds season-to-date aggregates (excluding current week) for fantasy points, targets, and carries.
    """
    print('Adding season-to-date aggregates...')
    df = df.sort_values(by=["player_id", "season", "week"])
    
    def compute_season_to_date(df: pd.DataFrame, col_name: str) -> pd.Series:
        df["week"] = df["week"].astype(int)
        df = df.sort_values(by=["player_id", "season", "week"])
        result = (
            df
            .groupby(["player_id", "season"])[col_name]
            .transform(lambda x: x.shift(1).expanding().mean())
        )
        return result

    df["std_fantasy_points"] = compute_season_to_date(df, "fantasy_points")
    df["std_targets"] = compute_season_to_date(df, "targets")
    df["std_carries"] = compute_season_to_date(df, "carries")
    df["std_passing_yards"] = compute_season_to_date(df, "passing_yards")
    df["std_rushing_yards"] = compute_season_to_date(df, "rushing_yards")
    df["std_receiving_yards"] = compute_season_to_date(df, "receiving_yards")
    df["std_passing_tds"] = compute_season_to_date(df, "passing_tds")
    df["std_rushing_tds"] = compute_season_to_date(df, "rushing_tds")
    df["std_receiving_tds"] = compute_season_to_date(df, "receiving_tds")

    df.drop_duplicates(inplace = True)

    return df

def add_3wk_rolling_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 3-week rolling averages (excluding current week) for selected stats.
    Requires: player_id, season, week columns.
    """
    print('Adding 3-week rolling averages...')

    df["week"] = df["week"].astype(int)
    df = df.sort_values(by=["player_id", "season", "week"]).reset_index(drop=True)

    cols = [
        "fantasy_points", "targets", "carries",
        "passing_yards", "rushing_yards", "receiving_yards",
        "passing_tds", "rushing_tds", "receiving_tds"
    ]

    for col in cols:
        rolling_col_name = f"{col}_3wk_avg"
        df[rolling_col_name] = (
            df.groupby(["player_id", "season"])[col]
            .transform(lambda x: x.shift(1).rolling(window=3, min_periods=3).mean())
        )

    return df


def add_5wk_rolling_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 5-week rolling averages (excluding current week) for selected stats.
    Requires: player_id, season, week columns.
    """
    print('Adding 5-week rolling averages...')

    df = df.copy()
    df["week"] = df["week"].astype(int)
    df = df.sort_values(by=["player_id", "season", "week"]).reset_index(drop=True)

    cols = [
        "fantasy_points", "targets", "carries",
        "passing_yards", "rushing_yards", "receiving_yards",
        "passing_tds", "rushing_tds", "receiving_tds"
    ]

    for col in cols:
        rolling_col_name = f"{col}_5wk_avg"
        df[rolling_col_name] = (
            df.groupby(["player_id", "season"])[col]
            .transform(lambda x: x.shift(1).rolling(window=5, min_periods=5).mean())
        )

    return df