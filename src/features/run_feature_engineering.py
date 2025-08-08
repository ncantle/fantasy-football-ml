import os
import logging
import argparse
from sqlalchemy import create_engine
from dotenv import load_dotenv
from src.features.rolling_features import add_season_to_date_aggregates, add_3wk_rolling_averages, add_5wk_rolling_averages
from src.features.home_away_features import add_home_away_rolling_and_std_averages
from src.features.base_features import generate_base_features
from src.features.opponent_avg_fantasy_points import generate_opponent_avg_fantasy_points, generate_opponent_avg_fantasy_points_with_rolling
from src.features.split_features_by_position import split_features_by_position

def main():
    logging.info('Running feature engineering pipeline...')
    load_dotenv()
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)

    df = generate_base_features(engine)
    df = add_season_to_date_aggregates(df)
    df = add_3wk_rolling_averages(df)
    df = add_5wk_rolling_averages(df)
    df = add_home_away_rolling_and_std_averages(df)
    df = generate_opponent_avg_fantasy_points(df)
    df = generate_opponent_avg_fantasy_points_with_rolling(df)

    df.to_csv('data/processed/features.csv', index=False)
    df.to_sql('features', engine, if_exists='replace', index=False)

    qb_df, rb_df, wr_df, te_df = split_features_by_position(df, engine)

    print("Feature engineering completed successfully!")
    logging.info('Feature engineering pipeline completed successfully.')

if __name__ == "__main__":
    main()