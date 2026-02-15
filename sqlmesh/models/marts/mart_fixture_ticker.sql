MODEL (
    name epl.mart_fixture_ticker,
    kind FULL,
    description 'Fixture ticker — upcoming fixtures for all teams with difficulty ratings. Use for planning transfers 3-5 GWs ahead.'
);

SELECT
    fd.team_id,
    t.team_name,
    t.short_name,
    fd.gameweek,
    fd.fixture_order,
    fd.opponent_name,
    fd.venue,
    fd.fdr,
    fd.opponent_form,
    fd.opponent_attack,
    fd.opponent_defense_weakness,
    fd.avg_fdr_next_3,
    fd.avg_fdr_next_5,

    -- Color coding for easy scanning
    CASE
        WHEN fd.fdr <= 2 THEN 'easy'
        WHEN fd.fdr = 3 THEN 'medium'
        WHEN fd.fdr >= 4 THEN 'hard'
    END AS difficulty_label

FROM epl.int_fixture_difficulty fd
LEFT JOIN epl.stg_teams t ON fd.team_id = t.team_id
WHERE fd.fixture_order <= 8  -- next 8 fixtures
ORDER BY fd.team_id, fd.fixture_order
