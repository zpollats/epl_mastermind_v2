MODEL (
    name epl.stg_fixtures,
    kind VIEW,
    description 'All 380 PL fixtures with scores, FDR ratings, and match status.'
);

SELECT
    id                                  AS fixture_id,
    event                               AS gameweek,
    finished,
    kickoff_time,

    -- Home team
    team_h                              AS home_team_id,
    team_h_score                        AS home_goals,
    team_h_difficulty                    AS home_fdr,

    -- Away team
    team_a                              AS away_team_id,
    team_a_score                        AS away_goals,
    team_a_difficulty                    AS away_fdr,

    -- Match result
    CASE
        WHEN finished = false THEN 'scheduled'
        WHEN team_h_score > team_a_score THEN 'home_win'
        WHEN team_a_score > team_h_score THEN 'away_win'
        WHEN team_h_score = team_a_score THEN 'draw'
    END                                 AS result

FROM raw_fpl.fixtures
WHERE event IS NOT NULL  -- exclude unscheduled fixtures
