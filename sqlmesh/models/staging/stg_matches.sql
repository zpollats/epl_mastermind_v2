MODEL (
    name epl.stg_matches,
    kind VIEW,
    description 'PL match results from football-data.org — used for ELO ratings and deeper match stats.'
);

SELECT
    id                                                  AS match_id,
    matchday,
    utc_date                                            AS kickoff_time,
    status,

    -- Home team
    home_team->>'$.name'                                AS home_team_name,
    CAST(home_team->>'$.id' AS INTEGER)                 AS home_team_fd_id,
    CAST(score->>'$.fullTime.home' AS INTEGER)          AS home_goals,
    CAST(score->>'$.halfTime.home' AS INTEGER)          AS home_goals_ht,

    -- Away team
    away_team->>'$.name'                                AS away_team_name,
    CAST(away_team->>'$.id' AS INTEGER)                 AS away_team_fd_id,
    CAST(score->>'$.fullTime.away' AS INTEGER)          AS away_goals,
    CAST(score->>'$.halfTime.away' AS INTEGER)          AS away_goals_ht,

    -- Result
    CASE
        WHEN score->>'$.winner' = 'HOME_TEAM' THEN 'home_win'
        WHEN score->>'$.winner' = 'AWAY_TEAM' THEN 'away_win'
        WHEN score->>'$.winner' = 'DRAW' THEN 'draw'
        ELSE 'scheduled'
    END                                                 AS result

FROM football_data.pl_matches
WHERE status IN ('FINISHED', 'IN_PLAY', 'PAUSED', 'TIMED', 'SCHEDULED')