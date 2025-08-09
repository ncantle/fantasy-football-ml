import pandas as pd

def generate_opportunity_share_features(features_df):
    # Calculate team carries and targets per week
    team_opps = (
        features_df.groupby(["team_abbreviation", "season", "week"])[["carries", "targets"]]
        .sum()
        .rename(columns={"carries": "team_carries", "targets": "team_targets"})
        .reset_index()
    )
    team_opps["team_total_opps"] = team_opps["team_carries"] + team_opps["team_targets"]

    # Shift team_total_opps by 1 week so that current week value is never used
    team_opps["team_total_opps_lag1"] = (
        team_opps.sort_values(["team_abbreviation", "season", "week"])
        .groupby(["team_abbreviation", "season"])["team_total_opps"]
        .shift(1)
    )

    # Merge lagged team totals into player-level dataframe
    features_df = features_df.merge(
        team_opps[["team_abbreviation", "season", "week", "team_total_opps_lag1"]],
        on=["team_abbreviation", "season", "week"],
        how="left"
    )

    # Calculate opportunity share from lagged team totals (avoiding current week data)
    features_df["opportunity_share_lag1"] = (
        (features_df["carries"] + features_df["targets"]) / features_df["team_total_opps_lag1"]
    )

    # Rolling averages (all based on lagged values)
    features_df["opportunity_share_3wk_avg"] = (
        features_df.groupby(["player_id", "season"])["opportunity_share_lag1"]
        .transform(lambda x: x.rolling(window=3, min_periods=3).mean())
    )

    features_df["opportunity_share_5wk_avg"] = (
        features_df.groupby(["player_id", "season"])["opportunity_share_lag1"]
        .transform(lambda x: x.rolling(window=5, min_periods=5).mean())
    )

    features_df["opportunity_share_std_avg"] = (
        features_df.groupby(["player_id", "season"])["opportunity_share_lag1"]
        .transform(lambda x: x.expanding().mean())
    )

    return features_df