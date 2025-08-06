from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

def reset_database(engine):
    # Schema SQL
    schema_sql = """
    -- Drop tables in dependency order for safe resets
    DROP TABLE IF EXISTS weekly_stats;
    DROP TABLE IF EXISTS depth_chart;
    DROP TABLE IF EXISTS injuries;
    DROP TABLE IF EXISTS players;
    DROP TABLE IF EXISTS teams;
    DROP TABLE IF EXISTS games;
    DROP TABLE IF EXISTS weather;

    -- ---------------------------
    -- Teams Table
    -- ---------------------------
    CREATE TABLE teams (
        team_id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        abbreviation VARCHAR(10) NOT NULL UNIQUE
    );

    -- ---------------------------
    -- Players Table
    -- ---------------------------
    CREATE TABLE players (
        player_id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        position VARCHAR(10),
        team_id INT REFERENCES teams(team_id)
    );

    -- ---------------------------
    -- Depth Chart Table
    -- ---------------------------
    CREATE TABLE depth_chart (
        player_id INT REFERENCES players(player_id),
        team_id INT REFERENCES teams(team_id),
        position VARCHAR(10),
        depth_position TEXT,
        PRIMARY KEY (player_id, team_id)
    );

    -- ---------------------------
    -- Injuries Table
    -- ---------------------------
    CREATE TABLE injuries (
        player_id INT REFERENCES players(player_id),
        week INT NOT NULL,
        status VARCHAR(20),
        PRIMARY KEY (player_id, week)
    );

    -- ---------------------------
    -- Weekly Stats Table
    -- ---------------------------
    CREATE TABLE weekly_stats (
        player_id INT REFERENCES players(player_id),
        season INT NOT NULL,
        week INT NOT NULL,
        passing_yards FLOAT,
        rushing_yards FLOAT,
        receiving_yards FLOAT,
        touchdowns FLOAT,
        fantasy_points FLOAT,
        PRIMARY KEY (player_id, season, week)
    );

    -- ---------------------------
    -- Games Table
    -- ---------------------------
    CREATE TABLE games (
        game_id SERIAL PRIMARY KEY,
        season INT,
        week INT,
        home_team VARCHAR(50),
        away_team VARCHAR(50),
        stadium VARCHAR(100),
        game_date DATE,
        home_team_id INT REFERENCES teams(team_id),
        away_team_id INT REFERENCES teams(team_id)
    );

    -- ---------------------------
    -- Weekly Stats Table (for fantasy points, targets, carries, snaps)
    -- ---------------------------
    CREATE TABLE IF NOT EXISTS weekly_stats (
        weekly_stats_id SERIAL PRIMARY KEY,
        player_name VARCHAR NOT NULL,
        player_display_name VARCHAR,
        position VARCHAR(10),
        team_abbreviation VARCHAR(10) NOT NULL,
        opponent_team VARCHAR(10),
        attempts INT,
        completions INT,
        passing_yards FLOAT,
        passing_tds INT,
        targets INT,
        receptions INT,
        carries INT,
        rushing_yards FLOAT,
        rushing_tds INT,
        receiving_yards FLOAT,
        receiving_tds INT,
        target_share FLOAT,
        fantasy_points FLOAT
    );

    -- ---------------------------
    -- Weather Table
    -- ---------------------------
    CREATE TABLE IF NOT EXISTS weather (
        weather_id SERIAL PRIMARY KEY,
        season INT,
        week INT,
        stadium VARCHAR,
        temperature FLOAT,
        precipitation FLOAT,
        wind_speed FLOAT,
        dome BOOLEAN
    );

    -- ---------------------------
    -- Offensive Line Rankings Table
    -- ---------------------------
    CREATE TABLE offensive_line_rankings (
        season INT,
        week INT,
        team_id INT,
        run_block_rank INT,
        pass_block_rank INT,
        overall_rank INT,
        source TEXT,
        UNIQUE (season, week, team_id)
    );
    """

    # Execute the schema reset
    with engine.connect() as conn:
        conn.execute(text(schema_sql))
        print("PostgreSQL schema has been reset and re-created.")