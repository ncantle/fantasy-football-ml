-- Drop tables in dependency order for safe resets
DROP TABLE IF EXISTS weekly_stats;
DROP TABLE IF EXISTS depth_chart;
DROP TABLE IF EXISTS injuries;
DROP TABLE IF EXISTS players;
DROP TABLE IF EXISTS teams;

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
    home_team_id INT REFERENCES teams(team_id),
    away_team_id INT REFERENCES teams(team_id)
);

-- ---------------------------
-- Weekly Stats Table (for fantasy points, targets, carries, snaps)
-- ---------------------------
CREATE TABLE IF NOT EXISTS weekly_stats (
    weekly_stats_id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id),
    team_id INT REFERENCES teams(team_id),
    season INT NOT NULL,
    week INT NOT NULL,
    fantasy_points FLOAT,
    targets INT,
    carries INT,
    snaps INT
);