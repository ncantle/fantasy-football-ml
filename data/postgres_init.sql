-- Teams
CREATE TABLE IF NOT EXISTS teams (
    team_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    abbreviation TEXT UNIQUE NOT NULL
);

-- Players
CREATE TABLE IF NOT EXISTS players (
    player_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    position TEXT NOT NULL,
    team_id INT REFERENCES teams(team_id),
    birthdate DATE
);

-- Weekly Stats
CREATE TABLE IF NOT EXISTS weekly_stats (
    stat_id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id),
    season INT,
    week INT,
    passing_yards INT,
    rushing_yards INT,
    receiving_yards INT,
    touchdowns INT,
    fantasy_points FLOAT,
    UNIQUE(player_id, season, week)
);

-- Injuries
CREATE TABLE IF NOT EXISTS injuries (
    injury_id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id),
    season INT,
    week INT,
    status TEXT,
    details TEXT
);

-- Depth Charts
CREATE TABLE IF NOT EXISTS depth_chart (
    depth_id SERIAL PRIMARY KEY,
    team_id INT REFERENCES teams(team_id),
    player_id INT REFERENCES players(player_id),
    position TEXT,
    depth INT,
    season INT
);

-- ADP
CREATE TABLE IF NOT EXISTS adp (
    adp_id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id),
    season INT,
    week INT,
    average_pick FLOAT,
    source TEXT
);