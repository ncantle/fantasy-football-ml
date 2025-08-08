import pandas as pd
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from src.features.pass_rush_rate import generate_pass_rush_rate_features
from src.features.weather_features import generate_weather_features
from src.features.opportunity_share_with_rolling import generate_opportunity_share_features

def generate_base_features(engine):
    print("Generating base feature table.")

    weekly_stats_df = pd.read_sql('SELECT * FROM weekly_stats', engine)
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

    final_df = generate_weather_features(weekly_stats_features, engine)
    final_df = generate_pass_rush_rate_features(final_df)
    final_df = generate_opportunity_share_features(final_df)

    final_df["career_avg_fantasy_points"] = (
        final_df.groupby("player_id")["fantasy_points"]
        .apply(lambda x: x.shift(1).expanding().mean())
        .reset_index(drop=True)
    )

    final_df.to_sql('player_weekly_features', engine, if_exists='replace', index=False)
    logging.info("Base feature table created and stored.")

    return final_df

if __name__ == "__main__":
    load_dotenv()
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)

    print('Generating base features...')
    generate_base_features(engine)