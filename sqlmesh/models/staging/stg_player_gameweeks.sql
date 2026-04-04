MODEL (
    name epl.stg_player_gameweeks,
    kind VIEW,
    description 'Per-player per-gameweek stats from FPL element-summary endpoing. One row per player per gameweek played.'
);

SELECT 
    -- Identity
    element                                         AS player_id,
    round                                           AS gameweek,
    fixture                                         AS fixture_id,
    opponent_team                                   AS opponent_team_id,
    was_home,

    -- Match context
    kickoff_time,
    team_h_score                                    AS home_goals,
    team_a_score                                    AS away_goals,

    -- Points
    total_points,
    minutes,
    starts,

    -- Attacking output
    goals_scored,
    assists,
    CAST(COALESCE(expected_goals, '0') AS DOUBLE)                AS xg,
    CAST(COALESCE(expected_assists, '0') AS DOUBLE)              AS xa,
    CAST(COALESCE(expected_goal_involvements, '0') AS DOUBLE)    AS xgi,

    -- Defensive output
    clean_sheets,
    defensive_contribution,
    goals_conceded,
    CAST(COALESCE(expected_goals_conceded, '0') AS DOUBLE)       AS xgc,

    -- Bonus & BPS
    bonus,
    bps,

    -- ICT
    CAST(COALESCE(influence, '0') AS DOUBLE)    AS influence,
    CAST(COALESCE(creativity, '0') AS DOUBLE)   AS creativity,
    CAST(COALESCE(threat, '0') AS DOUBLE)       AS threat,
    CAST(COALESCE(ict_index, '0') AS DOUBLE)    AS ict_index,

    -- Price & ownership at that gameweek
    CAST(value AS DOUBLE) / 10.0                AS price,
    selected                                    AS selected_by,
    transfers_in,
    transfers_out,
    transfers_balance                           AS net_transfers,

    -- Cards & other
    yellow_cards,
    red_cards,
    penalties_saved,
    penalties_missed,
    own_goals,
    saves

FROM raw_fpl.player_gameweeks
WHERE minutes > 0