import pandas as pd
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

def generate_base_features(engine):
    logging.info("Generating base feature table.")

    weekly_stats_df = pd.read_sql('SELECT * FROM weekly_stats', engine)
    games_df = pd.read_sql('SELECT * FROM games', engine)
    weather_df = pd.read_sql('SELECT * FROM weather', engine)
    
    weekly_stats_features = weekly_stats_df[['player_id',
                    'player_name',
                    'player_display_name',
                    'position',
                    'team_abbreviation',
                    'season',
                    'opponent_team',
                    'week',
                    'attempts',
                    'completions',
                    'passing_yards',
                    'passing_tds',
                    'targets',
                    'receptions',
                    'carries',
                    'rushing_yards',
                    'rushing_tds',
                    'receiving_yards',
                    'receiving_tds',
                    'target_share',
                    'fantasy_points']].sort_values(by = ['player_name','season','week']).drop_duplicates().reset_index(drop = True)

    # Adding weather data to base feature table
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
    
    final_df.to_sql('player_weekly_features', engine, if_exists='replace', index=False)
    logging.info("Base feature table created and stored.")
    return final_df

if __name__ == "__main__":
    load_dotenv()
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)

    print('Generating base features...')
    generate_base_features(engine)