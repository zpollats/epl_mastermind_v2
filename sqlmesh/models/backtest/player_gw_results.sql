MODEL (
    name backtest.player_gw_results,
    kind VIEW,
    tags [backtest],
    grain (player_id, gameweek),
    description 'One row per eligible player per fully finished gameweek (double-gameweek fixtures summed), with model and crowd ranks within position. Eligible = has_min_history and a non-NULL score.'
);

WITH finished_gws AS (
    SELECT 
        g.gameweek 
    FROM 
        epl.stg_gameweeks AS g
    WHERE g.finished
),

player_gw AS (
    SELECT 
        s.player_id,
        s.gameweek,
        ANY_VALUE(s.position)              AS position,
        COUNT(*)                           AS fixtures,
        SUM(s.pick_score)                  AS pick_score,
        SUM(s.actual_points)               AS actual_points,
        MAX(s.selected_by_count)           AS selected_by_count,
        BOOL_AND(s.has_min_history)        AS has_min_history
    FROM epl.int_player_fixture_scores AS s 
    INNER JOIN finished_gws AS fg
        ON s.gameweek = fg.gameweek 
    WHERE s.finished
    GROUP BY s.player_id, s.gameweek
),

squad_quota AS (
    SELECT 
        q.position,
        q.picks
    FROM (VALUES ('GK', 2), ('DEF', 5), ('MID', 5), ('FWD', 3)) AS q(position, picks)
),

eligible AS (
    SELECT 
        pg.player_id,
        pg.gameweek,
        pg.position,
        pg.fixtures,
        pg.pick_score,
        pg.actual_points,
        pg.selected_by_count,
        q.picks
    FROM player_gw AS pg
    INNER JOIN squad_quota AS q 
        ON pg.position = q.position
    WHERE pg.has_min_history
        AND pg.pick_score IS NOT NULL
)

SELECT 
    e.player_id,
    e.gameweek,
    e.position,
    e.fixtures,
    e.pick_score,
    e.actual_points,
    e.selected_by_count,
    e.picks,
    ROW_NUMBER() OVER (PARTITION BY e.gameweek, e.position
                       ORDER BY e.pick_score DESC, e.player_id) AS model_rank,
    ROW_NUMBER() OVER (PARTITION BY e.gameweek, e.position 
                       ORDER BY e.selected_by_count DESC, e.player_id) AS crowd_rank,
    RANK() OVER (PARTITION BY e.gameweek, e.position ORDER BY e.pick_score)
        + (COUNT(*) OVER (PARTITION BY e.gameweek, e.position, e.pick_score) - 1) / 2.0 AS score_rank,
    RANK() OVER (PARTITION BY e.gameweek, e.position ORDER BY e.actual_points)
        + (COUNT(*) OVER (PARTITION BY e.gameweek, e.position, e.actual_points) -1) / 2.0 AS actual_rank
FROM eligible AS e 