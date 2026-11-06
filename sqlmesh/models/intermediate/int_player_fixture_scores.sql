MODEL (
    name epl.int_player_fixture_scores,
    kind VIEW,
    grain (player_id, fixture_id),
    description 'pick_score for every player x fixture (finished and upcoming), computed once from features as of the gameweek deadline. Finished rows carry actual outcomes for backtesting; upcoming rows have NULL outcomes. Position is current (simplification). No snapshot-only fields (availability, current price) by design.'
);

-- Step 1: finished fixtures the player was in the squad for.
-- int_player_form already has one row per player per finished fixture, with the correct
-- point-in-time team_id, so reuse
WITH past AS (
    SELECT
        pf.player_id,
        pf.fixture_id,
        pf.team_id,
        pf.total_points   AS actual_points,
        pf.minutes        AS actual_minutes,
        pf.selected_by    AS selected_by_count -- managers owning player in a gw
    FROM epl.int_player_form AS pf
),

-- Step 2: unfinished fixtures for the player's CURRENT team
upcoming AS (
    SELECT 
        p.player_id,
        fd.fixture_id,
        p.team_id,
        CAST(NULL AS INTEGER) AS actual_points,
        CAST(NULL AS INTEGER) AS actual_minutes,
        CAST(NULL AS INTEGER) AS selected_by_count
    FROM epl.stg_players AS p 
    INNER JOIN epl.int_fixture_difficulty AS fd
        ON p.team_id = fd.team_id
    WHERE NOT fd.finished
),

-- Step 3: Combine past and upcoming
spine AS (
    SELECT
        p.player_id,
        p.fixture_id,
        p.team_id,
        p.actual_points,
        p.actual_minutes,
        p.selected_by_count
    FROM past AS p 
    UNION ALL 
    SELECT
        u.player_id,
        u.fixture_id,
        u.team_id,
        u.actual_points,
        u.actual_minutes,
        u.selected_by_count
    FROM upcoming AS u 
),

-- Step 4: add in team and opponent context
with_context AS (
    SELECT
        s.player_id, 
        s.fixture_id,
        s.team_id,
        s.actual_points,
        s.actual_minutes,
        s.selected_by_count,
        fd.gameweek,
        fd.kickoff_time,
        fd.deadline_time,
        fd.finished,
        fd.opponent_id, 
        fd.opponent_name,
        fd.venue,
        fd.fdr,
        fd.team_cs_rate,
        fd.opponent_attack,
        fd.opponent_defense_weakness,
        fd.opponent_venue_attack,
        fd.opponent_venue_defense,
        fd.opponent_form_as_of
    FROM spine AS s 
    INNER JOIN epl.int_fixture_difficulty AS fd 
        ON s.team_id = fd.team_id 
       AND s.fixture_id = fd.fixture_id
),

-- Step 5: add in player form as of the deadline
with_form AS (
    SELECT
        wc.player_id, 
        wc.fixture_id,
        wc.team_id,
        wc.actual_points,
        wc.actual_minutes,
        wc.selected_by_count,
        wc.gameweek,
        wc.kickoff_time,
        wc.deadline_time,
        wc.finished,
        wc.opponent_id, 
        wc.opponent_name,
        wc.venue,
        wc.fdr,
        wc.team_cs_rate,
        wc.opponent_attack,
        wc.opponent_defense_weakness,
        wc.opponent_venue_attack,
        wc.opponent_venue_defense,
        wc.opponent_form_as_of,
        p.player_name,
        p.position,
        f.points_avg_last3,
        f.minutes_avg_last3,
        f.fixtures_played,
        f.kickoff_time AS player_form_as_of
    FROM with_context AS wc
    INNER JOIN epl.stg_players AS p 
        ON wc.player_id = p.player_id
    ASOF LEFT JOIN epl.int_player_form AS f 
        ON wc.player_id = f.player_id 
       AND wc.deadline_time > f.kickoff_time
)

SELECT
    wf.player_id, 
    wf.fixture_id,
    wf.team_id,
    wf.actual_points,
    wf.actual_minutes,
    wf.selected_by_count,
    wf.gameweek,
    wf.kickoff_time,
    wf.deadline_time,
    wf.finished,
    wf.opponent_id, 
    wf.opponent_name,
    wf.venue,
    wf.fdr,
    wf.team_cs_rate,
    wf.opponent_attack,
    wf.opponent_defense_weakness,
    wf.opponent_venue_attack,
    wf.opponent_venue_defense,
    wf.opponent_form_as_of,
    wf.player_name,
    wf.position,
    wf.points_avg_last3,
    wf.minutes_avg_last3,
    wf.fixtures_played,
    wf.player_form_as_of,
    COALESCE(wf.fixtures_played, 0) >= 3 AS has_min_history,
    CASE 
        WHEN wf.points_avg_last3 IS NULL OR wf.fdr IS NULL THEN NULL 
        WHEN wf.position IN ('FWD', 'MID') THEN
            (wf.points_avg_last3 * 2.0)
            + ((6.0 - wf.fdr) * 1.0)
            + (COALESCE(wf.opponent_venue_defense, wf.opponent_defense_weakness, 1.0) * 2.0)
        WHEN wf.position = 'DEF' THEN
            (wf.points_avg_last3 * 2.0)
            + ((6.0 - wf.fdr) * 1.0)
            + ((2.0 - COALESCE(wf.opponent_venue_attack, wf.opponent_attack, 1.0)) * 2.5)
            + (COALESCE(wf.team_cs_rate, 0.0) * 3.0)
        WHEN wf.position = 'GK' THEN
            (wf.points_avg_last3 * 2.0)
            + ((6.0 - wf.fdr) * 1.0)
            + ((2.0 - COALESCE(wf.opponent_venue_attack, wf.opponent_attack, 1.0)) * 3.0)
            + (COALESCE(wf.team_cs_rate, 0.0) * 4.0)
    END AS pick_score
FROM with_form AS wf