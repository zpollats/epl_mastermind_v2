MODEL (
    name epl.stg_players,
    kind VIEW,
    description 'Cleaned player snapshot from FPL API bootstrap-static endpoint. One row per player per pipeline run (current-season totals).'
);

SELECT
    -- Identity
    id                                          AS player_id,
    web_name                                    AS player_name,
    first_name,
    second_name,

    -- Position & team
    element_type                                AS position_id,
    CASE element_type
        WHEN 1 THEN 'GK'
        WHEN 2 THEN 'DEF'
        WHEN 3 THEN 'MID'
        WHEN 4 THEN 'FWD'
    END                                         AS position,
    team                                        AS team_id,

    -- Price (FPL stores £6.5m as 65)
    CAST(now_cost AS DOUBLE) / 10.0             AS price,

    -- Aggregate season stats
    COALESCE(total_points, 0)                   AS total_points,
    COALESCE(minutes, 0)                        AS minutes,
    COALESCE(goals_scored, 0)                   AS goals_scored,
    COALESCE(assists, 0)                        AS assists,
    COALESCE(clean_sheets, 0)                   AS clean_sheets,
    COALESCE(goals_conceded, 0)                 AS goals_conceded,
    COALESCE(own_goals, 0)                      AS own_goals,
    COALESCE(penalties_saved, 0)                AS penalties_saved,
    COALESCE(penalties_missed, 0)               AS penalties_missed,
    COALESCE(yellow_cards, 0)                   AS yellow_cards,
    COALESCE(red_cards, 0)                      AS red_cards,
    COALESCE(saves, 0)                          AS saves,
    COALESCE(bonus, 0)                          AS bonus,

    -- ICT index components
    CAST(COALESCE(influence, '0') AS DOUBLE)    AS influence,
    CAST(COALESCE(creativity, '0') AS DOUBLE)   AS creativity,
    CAST(COALESCE(threat, '0') AS DOUBLE)       AS threat,
    CAST(COALESCE(ict_index, '0') AS DOUBLE)    AS ict_index,
    COALESCE(bps, 0)                            AS bps,

    -- Expected stats
    CAST(COALESCE(expected_goals, '0') AS DOUBLE)                AS xg,
    CAST(COALESCE(expected_assists, '0') AS DOUBLE)              AS xa,
    CAST(COALESCE(expected_goal_involvements, '0') AS DOUBLE)    AS xgi,
    CAST(COALESCE(expected_goals_conceded, '0') AS DOUBLE)       AS xgc,

    -- Ownership & transfers
    CAST(COALESCE(selected_by_percent, '0') AS DOUBLE)  AS ownership_pct,
    COALESCE(transfers_in_event, 0)             AS transfers_in_gw,
    COALESCE(transfers_out_event, 0)            AS transfers_out_gw,

    -- Form & value
    CAST(COALESCE(form, '0') AS DOUBLE)         AS form,
    CAST(COALESCE(points_per_game, '0') AS DOUBLE)      AS points_per_game,
    CAST(COALESCE(ep_next, '0') AS DOUBLE)      AS expected_points_next,
    CAST(COALESCE(value_form, '0') AS DOUBLE)   AS value_form,
    CAST(COALESCE(value_season, '0') AS DOUBLE) AS value_season,

    -- Availability
    COALESCE(chance_of_playing_next_round, 100) AS chance_of_playing,
    status,
    news

FROM raw_fpl.players
WHERE status != 'u'  -- exclude unavailable/unknown players
