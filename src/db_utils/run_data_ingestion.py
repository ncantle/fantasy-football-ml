import subprocess
import os
import argparse
import tqdm.notebook as tqdm
from dotenv import load_dotenv

# Load .env for DB credentials
load_dotenv()

# List of ingestion scripts
INGESTION_SCRIPTS = [
    "src/data_ingestion/fetch_sleeper_players.py",
    "src/data_ingestion/fetch_sleeper_depth_chart.py",
    "src/data_ingestion/fetch_nflfastr.py",
    "src/data_ingestion/fetch_sleeper_injuries.py",
    "src/data_ingestion/fetch_schedule.py",
    "src/data_ingestion/export_player_weekly_stats.py",
    "src/data_ingestion/fetch_weather.py"
    ""
]

def run_script(script_path):
    print(f"\n🚀 Running {script_path} ...")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(f"⚠️  {script_path} encountered an error:")
        print(result.stderr)

def main(reset_db=False):
    if reset_db:
        print("\n🔄 Resetting PostgreSQL Schema...")
        run_script("src/db_utils/reset_database.py")

    print("\n🚀 Running Data Ingestion Pipeline...")
    for script in INGESTION_SCRIPTS:
        run_script(script)
    
    print("\n✅ Data Ingestion Pipeline Completed Successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data ingestion pipeline.")
    parser.add_argument("--reset-db", action="store_true", help="Reset PostgreSQL schema before ingestion")
    args = parser.parse_args()

    main(reset_db=args.reset_db)
