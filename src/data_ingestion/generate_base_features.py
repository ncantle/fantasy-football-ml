import pandas as pd
import logging

def generate_base_features(engine):
    # --- Create filtered materialized views ---
    weekly_stats_df = pd.read_sql('SELECT * FROM weekly_stats', engine)
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
    
    weekly_stats_features.to_sql('player_weekly_features', engine, if_exists='replace', index=False)

    logging.info("Base feature table created and stored.")