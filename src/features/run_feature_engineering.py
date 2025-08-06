import os
import logging
import argparse
from sqlalchemy import create_engine
from dotenv import load_dotenv
from src.features.rolling_features import add_season_to_date_aggregates, add_3wk_rolling_averages, add_5wk_rolling_averages
from src.features.home_away_features import add_home_away_rolling_and_std_averages
from src.features.base_features import df
from src.features.base_features import generate_base_features

def main():
    load_dotenv()
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)

    df = generate_base_features(engine)

    df = add_season_to_date_aggregates(df)
    df = add_3wk_rolling_averages(df)
    df = add_5wk_rolling_averages(df)

    df = add_home_away_rolling_and_std_averages(df, window=3)

    print(df.head())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data ingestion pipeline.")
    parser.add_argument("--reset-db", action="store_true", help="Reset PostgreSQL schema before ingestion")
    parser.add_argument('--fetch-weather-historic', action="store_false", help="Fetch historic weather data")
    args = parser.parse_args()

    main(reset_db=args.reset_db)