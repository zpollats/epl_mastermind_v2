MODEL (
    name backtest.summary,
    kind FULL,
    tags [backtest],
    grain (gameweek, position),
    description 'Overall summary of model performance by position'
);

SELECT
    COALESCE(m.position, 'ALL')                                AS position,
    MIN(m.gameweek)                                            AS gw_from,
    MAX(m.gameweek)                                            AS gw_to,
    COUNT(DISTINCT m.gameweek)                                 AS gameweeks,
    SUM(m.model_points) / SUM(m.picks_made)                    AS model_pts_per_pick,
    SUM(m.crowd_points) / SUM(m.picks_made)                    AS crowd_pts_per_pick,
    SUM(m.field_avg_points * m.picks_made) / SUM(m.picks_made) AS field_pts_per_pick,
    SUM(m.model_points) / SUM(m.crowd_points) - 1              AS lift_vs_crowd,
    AVG(m.spearman)                                            AS avg_spearman
FROM backtest.gw_metrics AS m 
GROUP BY ROLLUP (m.position)