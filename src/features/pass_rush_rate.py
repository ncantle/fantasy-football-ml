import pandas as pd

def generate_pass_rush_rate_features(final_df):
    team_weekly = (
        final_df.groupby(["team_abbreviation", "season", "week"])
        .agg({"attempts": "sum", "carries": "sum"})
        .reset_index()
    )

    team_weekly["pass_rate"] = team_weekly["attempts"] / (team_weekly["attempts"] + team_weekly["carries"])
    team_weekly["rush_rate"] = 1 - team_weekly["pass_rate"]

    # Step 2: Shift the values by 1 week per team/season
    team_weekly["pass_rate_lag1"] = (
        team_weekly.sort_values(["team_abbreviation", "season", "week"])
        .groupby(["team_abbreviation", "season"])["pass_rate"]
        .shift(1)
    )
    team_weekly["rush_rate_lag1"] = 1 - team_weekly["pass_rate_lag1"]

    # Step 3: Merge lagged values back into main df
    final_df = final_df.merge(
        team_weekly[["team_abbreviation", "season", "week", "pass_rate_lag1", "rush_rate_lag1"]],
        on=["team_abbreviation", "season", "week"],
        how="left"
    )

    return final_df