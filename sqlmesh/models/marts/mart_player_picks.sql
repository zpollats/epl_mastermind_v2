MODEL (
    name epl.mart_player_picks,
    kind FULL,
    description 'Weekly player analysis combining form, value, fixture difficulty, and ownership for FPL decisions.',
    audits (
        not_null(columns=[player_id, player_name, position]),
        unique(columns=[player_id])
    )
);

WITH players AS (
    SELECT * FROM epl.stg_players
),

teams AS (
    SELECT * FROM epl.stg_teams
),

-- Get each team's next fixture difficulty
next_fixture AS (
    SELECT DISTINCT ON (team_id)
        team_id,
        gameweek         AS next_gw,
        fdr              AS next_fdr,
        opponent_name    AS next_opponent,
        venue            AS next_venue,
        opponent_form,
        opponent_defense_weakness,
        opponent_clean_sheet_rate,
        avg_fdr_next_3,
        avg_fdr_next_5
    FROM epl.int_fixture_difficulty
    WHERE fixture_order = 1
    ORDER BY team_id
),

-- Get each team's latest rolling performance
team_perf AS (
    SELECT DISTINCT ON (team_id)
        team_id,
        form_5gw          AS team_form,
        attack_strength_5gw AS team_attack,
        defense_weakness_5gw AS team_defense_weakness,
        clean_sheet_rate_5gw AS team_cs_rate
    FROM epl.int_team_performance
    ORDER BY team_id, gameweek DESC
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

    -- Next fixture context
    nf.next_gw,
    nf.next_opponent,
    nf.next_venue,
    nf.next_fdr,
    nf.avg_fdr_next_3,
    nf.avg_fdr_next_5,
    nf.opponent_form,
    nf.opponent_defense_weakness,
    nf.opponent_clean_sheet_rate,

    -- Team context
    tp.team_form,
    tp.team_attack,
    tp.team_defense_weakness,
    tp.team_cs_rate,

    -- Availability
    p.chance_of_playing,
    p.status,
    p.news,

    -- Composite scores (higher = better pick)
    -- Form + easy fixtures + good value = strong pick
    CASE
        WHEN p.form > 0 AND nf.next_fdr IS NOT NULL
        THEN (p.form * 2.0) + ((6.0 - nf.next_fdr) * 1.5) + p.value_form
        ELSE 0.0
    END                                         AS pick_score,

    -- Differential flag: high form, low ownership
    CASE
        WHEN p.form >= 5.0 AND p.ownership_pct < 10.0 THEN true
        ELSE false
    END                                         AS is_differential

FROM players p
LEFT JOIN teams t ON p.team_id = t.team_id
LEFT JOIN next_fixture nf ON p.team_id = nf.team_id
LEFT JOIN team_perf tp ON p.team_id = tp.team_id
WHERE
    p.minutes > 0  -- has played this season
    AND p.chance_of_playing >= 50  -- reasonably likely to play
