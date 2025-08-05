import subprocess
import os
import argparse
import pandas as pd
import datetime
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
from src.db_utils.reset_database import reset_database
from src.data_ingestion.fetch_sleeper_players import fetch_sleeper_players
from src.data_ingestion.fetch_sleeper_depth_chart import fetch_sleeper_depth_chart
from src.data_ingestion.fetch_nflfastr import fetch_nflfastr
from src.data_ingestion.fetch_sleeper_injuries import fetch_sleeper_injuries
from src.data_ingestion.fetch_schedule import fetch_schedule_for_seasons
from src.data_ingestion.export_player_weekly_stats import fetch_and_store_weekly_stats
from src.data_ingestion.fetch_weather import fetch_weather_historic
from src.data_ingestion.generate_base_features import generate_base_features
from src.db_utils.reset_views import reset_views_and_indexes
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))


def main(start_year = datetime.datetime.now().year - 4, end_year = datetime.datetime.now().year, reset_db=False, fetch_weather_historic = False):
    load_dotenv()
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)

    if reset_db:
        reset_database(engine)

    fetch_sleeper_players(engine)
    fetch_sleeper_depth_chart(engine)
    fetch_nflfastr(start_year = start_year, end_year = end_year, engine = engine)
    fetch_sleeper_injuries(engine)
    fetch_schedule_for_seasons(start_year=start_year, end_year=end_year, engine=engine)
    fetch_and_store_weekly_stats(engine, start_year=start_year, end_year=end_year, parquet_folder = 'data/raw/nflfastr/')

    if fetch_weather_historic:
        fetch_weather_historic(engine)

    generate_base_features(engine)

    print("Data ingestion pipeline completed successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data ingestion pipeline.")
    parser.add_argument("--reset-db", action="store_true", help="Reset PostgreSQL schema before ingestion")
    parser.add_argument('--fetch-weather-historic', action="store_false", help="Fetch historic weather data")
    args = parser.parse_args()

    main(reset_db=args.reset_db)