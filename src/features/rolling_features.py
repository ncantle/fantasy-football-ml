import pandas as pd

def add_season_to_date_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds season-to-date aggregates (excluding current week) for fantasy points, targets, and carries.
    """
    df = df.sort_values(by=["player_id", "season", "week"])

    def compute_season_to_date(col_name):
        return (
            df.groupby(["player_id", "season"])[col_name]
            .apply(lambda x: x.shift(1).cumsum())
            .reset_index(drop=True)
        )

    df["std_fantasy_points"] = compute_season_to_date("fantasy_points")
    df["std_targets"] = compute_season_to_date("targets")
    df["std_carries"] = compute_season_to_date("carries")

    return df


def add_3wk_rolling_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 3-week rolling averages (excluding current week) for fantasy points, targets, and carries.
    Assumes the DataFrame contains: player_id, season, week, fantasy_points, targets, carries.
    """

    df = df.sort_values(by=["player_id", "season", "week"])

    for col in ["fantasy_points", "targets", "carries"]:
        rolling_col_name = f"{col}_3wk_avg"
        df[rolling_col_name] = (
            df.groupby(["player_id", "season"])[col]
            .apply(lambda x: x.shift(1).rolling(window=3, min_periods=1).mean())
            .reset_index(drop=True)
        )

    return df

def add_5wk_rolling_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 3-week rolling averages (excluding current week) for fantasy points, targets, and carries.
    Assumes the DataFrame contains: player_id, season, week, fantasy_points, targets, carries.
    """

    df = df.sort_values(by=["player_id", "season", "week"])

    for col in ["fantasy_points", "targets", "carries"]:
        rolling_col_name = f"{col}_5wk_avg"
        df[rolling_col_name] = (
            df.groupby(["player_id", "season"])[col]
            .apply(lambda x: x.shift(1).rolling(window=5, min_periods=1).mean())
            .reset_index(drop=True)
        )

    return df