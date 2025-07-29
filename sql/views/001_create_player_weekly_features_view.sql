CREATE MATERIALIZED VIEW IF NOT EXISTS player_weekly_features AS
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
FROM weekly_stats ws
LEFT JOIN players p ON ws.player_id = p.player_id
LEFT JOIN teams t ON p.team_id = t.team_id
LEFT JOIN games g ON ws.season = g.season AND ws.week = g.week
LEFT JOIN depth_chart d ON p.player_id = d.player_id
LEFT JOIN injuries i 
    ON p.player_id = i.player_id 
    AND ws.week = i.week 
    AND ws.season = i.season;
