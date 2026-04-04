MODEL (
    name epl.int_player_rolling_backtest,
    kind VIEW,
    description 'Player-level rolling stats per gameweek for backtesting. Calculates form and performance metrics using only data available at that point in the season.'
);

WITH player_gw as (
    SELECT
        pg.player_id,
        pg.gameweek,
        pg.opponent_team_id,
        pg.was_home,
        pg.total_points,
        pg.minutes,
        pg.goals_scored,
        pg.assists,
        pg.clean_sheets,
        pg.bonus,
        pg.xg,
        pg.xa,
        pg.xgi,
        pg.xgc,
        pg.ict_index,
        pg.price,
        pg.selected_by,
        pg.net_transfers,
        pg.defensive_contribution,
        pg.yellow_cards,
        pg.red_cards,

        -- Player identity from stg_players (position, team)
        p.position,
        p.team_id,
        p.player_name
    
    FROM epl.stg_player_gameweeks pg
    INNER JOIN epl.stg_players p ON pg.player_id = p.player_id
)

SELECT
    player_id, 
    player_name,
    position,
    team_id,
    gameweek,
    opponent_team_id,
    was_home,
    total_points,
    minutes,
    price,
    selected_by,

    -- Rolling points per game (last 3 GWs)
    AVG(total_points) OVER (
        PARTITION BY player_id ORDER BY gameweek
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS form_3gw,

    -- Rolling points per game (last 5 GWs)
    AVG(total_points) OVER (
        PARTITION BY player_id ORDER BY gameweek
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS form_5gw,

    -- Rolling xGI per game (last 3 GWs)
    AVG(xgi) OVER (
        PARTITION BY player_id ORDER BY gameweek
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS xgi_rolling_3gw,

    -- Rolling xGC per game (last 3 GWs)
    AVG(xgc) OVER (
        PARTITION BY player_id ORDER BY gameweek
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS xgc_rolling_3gw,

    -- Rolling yellow cards per game (last 5 GWs)
    AVG(yellow_cards) OVER (
        PARTITION BY player_id ORDER BY gameweek
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS yellow_rolling_5gw,

    -- Rolling red cards (last 5 GWs) 
    SUM(red_cards) OVER (
        PARTITION BY player_id ORDER BY gameweek
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS red_rolling_5gw,

    -- Rolling bonus per game (last 5 GWs)
    AVG(bonus) OVER (
        PARTITION BY player_id ORDER BY gameweek
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS bonus_rolling_5gw,

    -- Rolling ICT index (last 3 GWs)
    AVG(ict_index) OVER (
        PARTITION BY player_id ORDER BY gameweek
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS ict_rolling_3gw,

    -- Rolling defensive contributions (last 3 GWs)
    AVG(defensive_contribution) OVER (
        PARTITION BY player_id ORDER BY gameweek
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS defcon_rolling_3gw,

    -- Season cumulative points up to the previous GW
    SUM(total_points) OVER (
        PARTITION BY player_id ORDER BY gameweek
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS season_points_before,

    -- Games played up to previous GW
    COUNT(*) OVER (
        PARTITION BY player_id ORDER BY gameweek
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS games_played_before

FROM player_gw