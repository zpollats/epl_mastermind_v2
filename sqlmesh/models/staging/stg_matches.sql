MODEL (
    name epl.stg_matches,
    kind VIEW,
    description 'PL match results from football-data.org — used for ELO ratings and deeper match stats.'
);

SELECT
    id                                          AS match_id,
    matchday,
    utc_date                                    AS kickoff_time,
    status,

    -- Home team
    home_team__name                             AS home_team_name,
    home_team__id                               AS home_team_fd_id,
    COALESCE(score__full_time__home, 0)         AS home_goals,
    COALESCE(score__half_time__home, 0)         AS home_goals_ht,

    -- Away team
    away_team__name                             AS away_team_name,
    away_team__id                               AS away_team_fd_id,
    COALESCE(score__full_time__away, 0)         AS away_goals,
    COALESCE(score__half_time__away, 0)         AS away_goals_ht,

    -- Result
    CASE
        WHEN score__winner = 'HOME_TEAM' THEN 'home_win'
        WHEN score__winner = 'AWAY_TEAM' THEN 'away_win'
        WHEN score__winner = 'DRAW' THEN 'draw'
        ELSE 'scheduled'
    END                                         AS result

FROM raw_football_data.matches
WHERE status IN ('FINISHED', 'IN_PLAY', 'PAUSED', 'TIMED', 'SCHEDULED')
