import os
import subprocess
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

def fetch_nflfastr(start_year: int, end_year: int, engine):
    # --- Settings ---
    seasons = list(range(start_year, end_year + 1))
    base_url = 'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{year}.parquet'
    local_dir = "data/raw/nflfastr"
    os.makedirs(local_dir, exist_ok=True)


    # --- Download with curl to ensure redirects handled ---
    def download_file_curl(url, output_path):
        if os.path.exists(output_path):
            print(f"Already exists: {output_path}")
            return
        print(f"Downloading: {url}")
        subprocess.run(['curl', '-L', url, '-o', output_path], check=True)
        print(f"Saved: {output_path}")


    # --- Download files for each season ---
    files = []
    for season in seasons:
        url = base_url.format(year=season)
        local_path = os.path.join(local_dir, f"player_stats_{season}.parquet")
        download_file_curl(url, local_path)
        files.append(local_path)


    # --- Ingest with DuckDB ---
    dfs = []
    for file in files:
        if os.path.getsize(file) < 1000:
            print(f"File {file} is too small to be valid. Aborting.")
            continue
        print(f"Processing {file} ...")
        query = f"""
            SELECT 
                season, 
                week, 
                player_name AS name, 
                passing_yards, 
                rushing_yards, 
                receiving_yards, 
                passing_tds + rushing_tds + receiving_tds AS touchdowns, 
                fantasy_points_ppr AS fantasy_points 
            FROM read_parquet('{file}')
        """
        df = duckdb.query(query).to_df()
        dfs.append(df)

    combined_df = pd.concat(dfs)
    print(f"Combined records: {len(combined_df)}")


    # --- Map player_id from players table ---
    players_df = pd.read_sql('SELECT player_id, name FROM players', engine)
    merged_df = combined_df.merge(players_df, how='left', on='name')

    # --- Report unmatched players ---
    unmatched = merged_df[merged_df['player_id'].isnull()]
    if not unmatched.empty:
        print("Unmatched players (not in your `players` table):")
        print(unmatched['name'].dropna().unique())

    # --- Filter out unmatched for DB insert ---
    final_df = merged_df[merged_df['player_id'].notnull()]

    # --- Prepare for Postgres insert ---
    final_df = final_df[['player_id', 'season', 'week', 'passing_yards', 'rushing_yards', 'receiving_yards', 'touchdowns', 'fantasy_points']]

    # --- Insert into Postgres ---
    final_df.drop_duplicates().to_sql('weekly_stats', engine, if_exists='replace', index=False)
    print(f"{len(final_df)} rows successfully ingested into `weekly_stats`.")
