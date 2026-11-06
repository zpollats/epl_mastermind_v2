MODEL (
    name epl.mart_player_picks,
    kind FULL,
    description 'One row per player; pick_score for the next gameweek, summed across double gameweek fixtures.'
);

WITH next_gw AS (
    SELECT
        gameweek
    FROM epl.stg_gameweeks 
    WHERE is_next
),

gw_scores AS (
    SELECT
        s.player_id,
        COUNT(*)                                                   AS fixtures_in_gw,
        STRING_AGG(s.opponent_name || ' (' || s.venue || ')', ', '
                   ORDER BY s.kickoff_time)                        AS opponents,
        AVG(s.fdr)                                                 AS avg_fdr,
        SUM(s.pick_score)                                          AS pick_score 
    FROM epl.int_player_fixture_scores AS s 
    INNER JOIN next_gw AS g 
        ON s.gameweek = g.gameweek 
    GROUP BY s.player_id
)

SELECT
    -- Player identity
    p.player_id,
    p.player_name,
    p.position,
    t.team_name,
    t.short_name AS team_short,

    -- Price & ownership
    p.price,
    p.ownership_pct,
    p.transfers_in_gw,
    p.transfers_out_gw,
    p.transfers_in_gw - p.transfers_out_gw      AS net_transfers,

    -- Season performance
    p.total_points,
    p.minutes,
    p.goals_scored,
    p.assists,
    p.clean_sheets,
    p.defensive_contribution,
    p.bonus,

    -- Form & expected
    p.form,
    p.points_per_game,
    p.expected_points_next,
    p.xg,
    p.xa,
    p.xgi,
    p.ict_index,

    -- Value metrics
    p.value_form,
    p.value_season,
    CASE
        WHEN p.points_per_game > 0 THEN p.price / p.points_per_game
        ELSE 999.0
    END                                         AS cost_per_point,

    -- Availability
    p.chance_of_playing,
    p.status,
    p.news,

    -- Differential flag: high form, low ownership
    CASE
        WHEN p.form >= 5.0 AND p.ownership_pct < 10.0 THEN true
        ELSE false
    END                                         AS is_differential,

    -- gw_scores computed values
    COALESCE(gs.fixtures_in_gw, 0) AS fixtures_in_gw,
    gs.opponents, 
    gs.avg_fdr,
    gs.pick_score

FROM epl.stg_players AS p
LEFT JOIN epl.stg_teams AS t 
    ON p.team_id = t.team_id
LEFT JOIN gw_scores AS gs
    ON p.player_id = gs.player_id
WHERE p.minutes > 0 AND p.chance_of_playing >= 50
