MODEL (
    name epl.mart_backtest_picks,
    kind FULL,
    description 'Retroactive pick scores for every player-gameweek using only pre-gameweek data. Used to validate the pick_score formula and train ML models.'
);

WITH player_rolling AS (
    SELECT * FROM epl.int_player_rolling_backtest
),

-- Get fixture context for each player-gameweek (FDR, venue)
fixture_context AS (
    SELECT
        home_team_id    AS team_id,
        away_team_id    AS opponent_id,
        gameweek,
        home_fdr        AS fdr,
        'home'          AS venue
    FROM epl.stg_fixtures
    WHERE finished = true

    UNION ALL

    SELECT
        away_team_id    AS team_id,
        home_team_id    AS opponent_id,
        gameweek,
        away_fdr        AS fdr,
        'away'          AS venue
    FROM epl.stg_fixtures
    WHERE finished = true
),

-- Get opponent's lagged rolling stats
-- If player is home, opponent is away — use opponent's away stats
-- If player is away, opponent is home — use opponent's home stats
opponent_stats AS (
    SELECT DISTINCT ON (team_id, gameweek)
        team_id,
        gameweek,
        form_5gw,
        attack_strength_5gw,
        defense_weakness_5gw,
        clean_sheet_rate_5gw,
        home_attack_3gw,
        home_defense_3gw,
        home_cs_rate_3gw,
        away_attack_3gw,
        away_defense_3gw,
        away_cs_rate_3gw
    FROM epl.int_team_performance_backtest
    ORDER BY team_id, gameweek, venue
),

-- Own team's lagged CS rate for defender/GK scoring
own_team_stats AS (
    SELECT DISTINCT ON (team_id, gameweek)
        team_id,
        gameweek,
        clean_sheet_rate_5gw AS team_cs_rate
    FROM epl.int_team_performance_backtest
    ORDER BY team_id, gameweek, venue
),

enriched AS (
    SELECT
        pr.player_id,
        pr.player_name,
        pr.position,
        pr.team_id,
        pr.gameweek,
        pr.total_points,
        pr.minutes,
        pr.price,
        pr.selected_by,

        -- Player rolling stats (lagged)
        pr.form_3gw,
        pr.form_5gw,
        pr.xgi_rolling_3gw,
        pr.xgc_rolling_3gw,
        pr.defcon_rolling_3gw,
        pr.bonus_rolling_5gw,
        pr.ict_rolling_3gw,
        pr.yellow_rolling_5gw,
        pr.season_points_before,
        pr.games_played_before,

        -- Fixture context
        fc.fdr,
        fc.venue,
        fc.opponent_id,

        -- Opponent blended stats (lagged)
        opp.form_5gw                AS opponent_form,
        opp.attack_strength_5gw     AS opponent_attack,
        opp.defense_weakness_5gw    AS opponent_defense_weakness,
        opp.clean_sheet_rate_5gw    AS opponent_clean_sheet_rate,

        -- Opponent venue-specific stats (lagged)
        CASE WHEN fc.venue = 'home' THEN opp.away_attack_3gw
             WHEN fc.venue = 'away' THEN opp.home_attack_3gw
        END AS opponent_venue_attack,

        CASE WHEN fc.venue = 'home' THEN opp.away_defense_3gw
             WHEN fc.venue = 'away' THEN opp.home_defense_3gw
        END AS opponent_venue_defense,

        CASE WHEN fc.venue = 'home' THEN opp.away_cs_rate_3gw
             WHEN fc.venue = 'away' THEN opp.home_cs_rate_3gw
        END AS opponent_venue_cs_rate,

        -- Own team CS rate (lagged)
        ot.team_cs_rate

    FROM player_rolling pr
    INNER JOIN fixture_context fc
        ON pr.team_id = fc.team_id
        AND pr.gameweek = fc.gameweek
    LEFT JOIN opponent_stats opp
        ON fc.opponent_id = opp.team_id
        AND fc.gameweek = opp.gameweek
    LEFT JOIN own_team_stats ot
        ON pr.team_id = ot.team_id
        AND pr.gameweek = ot.gameweek
)

SELECT
    player_id,
    player_name,
    position,
    team_id,
    gameweek,
    total_points,
    minutes,
    price,
    selected_by,
    form_3gw,
    form_5gw,
    xgi_rolling_3gw,
    xgc_rolling_3gw,
    defcon_rolling_3gw,
    bonus_rolling_5gw,
    ict_rolling_3gw,
    yellow_rolling_5gw,
    season_points_before,
    games_played_before,
    fdr,
    venue,
    opponent_id,
    opponent_form,
    opponent_attack,
    opponent_defense_weakness,
    opponent_clean_sheet_rate,
    opponent_venue_attack,
    opponent_venue_defense,
    opponent_venue_cs_rate,
    team_cs_rate,

    -- Position-aware pick score (same formula as mart_player_picks)
    CASE
        WHEN form_3gw IS NULL OR fdr IS NULL THEN NULL

        WHEN position IN ('FWD', 'MID') THEN
            (form_3gw * 2.0)
            + ((6.0 - fdr) * 1.0)
            + (COALESCE(opponent_venue_defense, opponent_defense_weakness, 1.0) * 2.0)

        WHEN position = 'DEF' THEN
            (form_3gw * 2.0)
            + ((6.0 - fdr) * 1.0)
            + ((2.0 - COALESCE(opponent_venue_attack, opponent_attack, 1.0)) * 2.5)
            + (COALESCE(team_cs_rate, 0.0) * 3.0)

        WHEN position = 'GK' THEN
            (form_3gw * 2.0)
            + ((6.0 - fdr) * 1.0)
            + ((2.0 - COALESCE(opponent_venue_attack, opponent_attack, 1.0)) * 3.0)
            + (COALESCE(team_cs_rate, 0.0) * 4.0)
    END AS pick_score,

    -- Points per game baseline ("class" metric)
    CASE
        WHEN games_played_before > 0
        THEN CAST(season_points_before AS DOUBLE) / games_played_before
        ELSE NULL
    END AS points_per_game_before

FROM enriched
WHERE gameweek >= 5