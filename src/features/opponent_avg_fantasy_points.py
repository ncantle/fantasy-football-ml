def generate_opponent_avg_fantasy_points(final_df):
    print('Generating opponent average fantasy points...')
    # Group by opponent team, season, and week to get total fantasy points they allowed that week
    points_allowed_weekly = (
        final_df.groupby(["opponent_team", "season", "week"])["fantasy_points"]
        .sum()
        .reset_index()
        .rename(columns={
            "opponent_team": "team_abbreviation",  # opponent is the defense
            "fantasy_points": "fantasy_points_allowed"
        })
    )

    # Calculate season average fantasy points allowed (up to the current week, excluding current)
    points_allowed_weekly = points_allowed_weekly.sort_values(by=["team_abbreviation", "season", "week"])
    points_allowed_weekly["fantasy_points_allowed_avg"] = (
        points_allowed_weekly
        .groupby(["team_abbreviation", "season"])["fantasy_points_allowed"]
        .apply(lambda x: x.shift(1).expanding().mean())
        .reset_index(drop=True)
    )

    final_df = final_df.merge(
    points_allowed_weekly[["team_abbreviation", "season", "week", "fantasy_points_allowed_avg"]],
    left_on=["opponent_team", "season", "week"],
    right_on=["team_abbreviation", "season", "week"],
    how="left"
    )

    # Optional: rename or drop the merge key to avoid confusion
    final_df = final_df.drop(columns=["team_abbreviation_y"])
    final_df = final_df.rename(columns={"team_abbreviation_x": "team_abbreviation"})

    return final_df

def generate_opponent_avg_fantasy_points_with_rolling(final_df):
    print('Generating opponent average fantasy points with rolling averages...')

    positions = ["QB", "RB", "WR", "TE"]
    pos_df = final_df[final_df["position"].isin(positions)].copy()

    weekly_points_allowed_by_pos = (
        pos_df.groupby(["opponent_team", "season", "week", "position"])["fantasy_points"]
        .sum()
        .reset_index()
        .rename(columns={"opponent_team": "team_abbreviation", "fantasy_points": "fantasy_points_allowed"})
    )

    weekly_points_pivot = weekly_points_allowed_by_pos.pivot_table(
        index=["team_abbreviation", "season", "week"],
        columns="position",
        values="fantasy_points_allowed",
        fill_value=0
    ).reset_index()

    weekly_points_pivot.columns.name = None
    weekly_points_pivot = weekly_points_pivot.rename(columns={
        "QB": "fantasy_points_allowed_QB",
        "RB": "fantasy_points_allowed_RB",
        "WR": "fantasy_points_allowed_WR",
        "TE": "fantasy_points_allowed_TE"
    })

    weekly_points_pivot = weekly_points_pivot.sort_values(by=["team_abbreviation", "season", "week"])

    for pos in ["QB", "RB", "WR", "TE"]:
        col = f"fantasy_points_allowed_{pos}"
        avg_col = f"{col}_avg"
        weekly_points_pivot[avg_col] = (
            weekly_points_pivot
            .groupby(["team_abbreviation", "season"])[col]
            .transform(lambda x: x.shift(1).expanding().mean())
        )

    # Compute 3-week rolling averages per position, lagged by 1 week
    for pos in ["QB", "RB", "WR", "TE"]:
        col = f"fantasy_points_allowed_{pos}"
        rolling_col = f"{col}_3wk_avg"
        weekly_points_pivot[rolling_col] = (
            weekly_points_pivot
            .groupby(["team_abbreviation", "season"])[col]
            .transform(lambda x: x.shift(1).rolling(window=3, min_periods=3).mean())
        )

    final_df = final_df.merge(
        weekly_points_pivot[["team_abbreviation", "season", "week",
                            'fantasy_points_allowed_QB_avg',
                            'fantasy_points_allowed_RB_avg',
                            'fantasy_points_allowed_WR_avg',
                            'fantasy_points_allowed_TE_avg',
                            "fantasy_points_allowed_QB_3wk_avg",
                            "fantasy_points_allowed_RB_3wk_avg",
                            "fantasy_points_allowed_WR_3wk_avg",
                            "fantasy_points_allowed_TE_3wk_avg"]],
        left_on=["opponent_team", "season", "week"],
        right_on=["team_abbreviation", "season", "week"],
        how="left"
    ).drop(columns=["team_abbreviation_y"]).rename(columns={"team_abbreviation_x": "team_abbreviation"})

    return final_df