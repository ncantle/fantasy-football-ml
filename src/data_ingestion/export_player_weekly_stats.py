import os
import pandas as pd
import logging
from sqlalchemy import text

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

    # --- Create filtered materialized views ---
    with engine.begin() as conn:
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS filtered_weekly_stats;"))
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS filtered_games;"))
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS filtered_injuries;"))

        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW filtered_weekly_stats AS
            SELECT * FROM weekly_stats WHERE season BETWEEN {start_year} AND {end_year};
        """))

        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW filtered_games AS
            SELECT * FROM games WHERE season BETWEEN {start_year} AND {end_year};
        """))

        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW filtered_injuries AS
            SELECT * FROM injuries WHERE season BETWEEN {start_year} AND {end_year};
        """))

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_filtered_ws ON filtered_weekly_stats(season, week, player_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_filtered_games ON filtered_games(season, week);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_filtered_injuries ON filtered_injuries(season, week, player_id);"))

        logging.info("✅ Filtered materialized views created and indexed.")

        # --- Create player_weekly_features view ---
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS player_weekly_features;"))
        conn.execute(text("""
            CREATE MATERIALIZED VIEW player_weekly_features AS
            SELECT
                ws.season,
                ws.week,
                p.player_id,
                p.name,
                t.abbreviation AS team,
                p.team_id,
                p.position,
                CASE 
                    WHEN p.team_id = g.home_team_id THEN g.away_team
                    ELSE g.home_team
                END AS opponent,
                ws.fantasy_points,
                ws.targets,
                ws.carries,
                d.depth_position,
                i.injury_status
            FROM filtered_weekly_stats ws
            LEFT JOIN players p ON ws.player_id = p.player_id
            LEFT JOIN teams t ON p.team_id = t.team_id
            LEFT JOIN filtered_games g ON ws.season = g.season AND ws.week = g.week
            LEFT JOIN depth_chart d ON p.player_id = d.player_id
            LEFT JOIN filtered_injuries i 
                ON p.player_id = i.player_id 
                AND ws.week = i.week 
                AND ws.season = i.season
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pwf ON player_weekly_features(season, week, player_id);"))

    logging.info("✅ player_weekly_features materialized view created and indexed.")

