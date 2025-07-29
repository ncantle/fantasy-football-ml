import os
import pandas as pd
import logging
from sqlalchemy import text

def fetch_and_store_weekly_stats(engine, start_year: int, end_year: int, parquet_folder):
    all_dfs = []
    seasons = list(range(start_year, end_year + 1))

    for season in seasons:
        try:
            file_path = os.path.join(parquet_folder, f"player_stats_{season}.parquet")
            if not os.path.exists(file_path):
                logging.warning(f"Parquet file for season {season} not found: {file_path}")
                continue

            logging.info(f"Loading stats for season {season}")
            df = pd.read_parquet(file_path)

            df = df.rename(columns={
                'season': 'season',
                'week': 'week',
                'player_id': 'player_id',
                'recent_team': 'team_abbreviation',
                'targets': 'targets',
                'carries': 'carries',
                'offense_snaps': 'snaps'
            })

            all_dfs.append(df)
        except Exception as e:
            logging.warning(f"Failed to process season {season}: {e}")
            continue

    if not all_dfs:
        logging.error("No data loaded from any season.")
        return

    weekly_stats_df = pd.concat(all_dfs, ignore_index=True).reset_index(drop=True)

    # Merge team_id from teams table
    teams_df = pd.read_sql('SELECT team_id, abbreviation FROM teams', engine)
    weekly_stats_df = weekly_stats_df.merge(teams_df, how='left', left_on='team_abbreviation', right_on='abbreviation')

    # Clean player_id
    weekly_stats_df['player_id'] = (
        weekly_stats_df['player_id']
        .astype(str)
        .str.replace(r'[^0-9]', '', regex=True)
        .replace('', pd.NA)
    )
    weekly_stats_df = weekly_stats_df.dropna(subset=['player_id', 'team_id'])
    weekly_stats_df['player_id'] = weekly_stats_df['player_id'].astype(int)
    weekly_stats_df['team_id'] = weekly_stats_df['team_id'].astype(int)

    # Final columns
    weekly_stats_df = weekly_stats_df[['season', 'week', 'player_id', 'team_id', 'fantasy_points', 'targets', 'carries']]

    # Ingest into PostgreSQL
    weekly_stats_df.to_sql('weekly_stats', engine, if_exists='replace', index=False)
    logging.info("✅ weekly_stats table ingested into PostgreSQL.")

    # # Refresh materialized view
    # with engine.connect() as conn:
    #     logging.info("🔁 Refreshing materialized view: player_weekly_features ...")
    #     conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY player_weekly_features;"))
    #     logging.info("✅ Materialized view refreshed.")
