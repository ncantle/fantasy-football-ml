import pandas as pd

# Compute total team opportunities per week
def generate_opportunity_share_features(features_df):
    team_opps = (
        features_df.groupby(["team_abbreviation", "season", "week"])[["carries", "targets"]]
        .sum()
        .rename(columns={"carries": "team_carries", "targets": "team_targets"})
        .reset_index()
    )
    team_opps["team_total_opps"] = team_opps["team_carries"] + team_opps["team_targets"]

    # Merge team totals into features_df
    features_df = features_df.merge(
        team_opps[["team_abbreviation", "season", "week", "team_total_opps"]],
        on=["team_abbreviation", "season", "week"],
        how="left"
    )

    # Compute current week opportunity share
    features_df["opportunity_share"] = (features_df["carries"] + features_df["targets"]) / features_df["team_total_opps"]

    # Lag opportunity share by one week per player/season
    features_df["opportunity_share_lag1"] = (
        features_df.sort_values(["player_id", "season", "week"])
        .groupby(["player_id", "season"])["opportunity_share"]
        .shift(1)
    )

    # Adding in rolling averages for opportunity share
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

    features_df.drop(columns = ['opportunity_share'], inplace = True)

    return features_df