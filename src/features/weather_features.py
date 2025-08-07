import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

def generate_weather_features(weekly_stats_features, engine):
    games_df = pd.read_sql('SELECT * FROM games', engine)
    weather_df = pd.read_sql('SELECT * FROM weather', engine)

    player_games = weekly_stats_features.merge(
        games_df,
        how = 'left',
        on = ['season', 'week']
    )

    # Filter rows where team_abbreviation matches either home or away team
    player_games = player_games[
        (player_games['team_abbreviation'] == player_games['home_team']) |
        (player_games['team_abbreviation'] == player_games['away_team'])
    ]

    player_games_with_weather = player_games.merge(
        weather_df,
        how = 'left',
        on = ['season', 'week', 'stadium']
    )

    player_games_with_weather['home_game'] = (
        player_games_with_weather['team_abbreviation'] == player_games_with_weather['home_team']
    )

    final_df = player_games_with_weather[
        weekly_stats_features.columns.tolist() + ['stadium', 'temperature', 'precipitation', 'wind_speed', 'dome', 'home_game']
    ].drop_duplicates().reset_index(drop = True)

    final_df["cold_game"] = final_df["temperature"] < 32
    final_df["windy_game"] = final_df["wind_speed"] > 20
    final_df["rain_game"] = final_df["precipitation"] > 0.3
    final_df["extreme_weather"] = final_df[["cold_game", "windy_game", "rain_game"]].any(axis=1)

    return final_df