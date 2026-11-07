MODEL (
    name backtest.gw_metrics,
    kind FULL,
    tag [backtest],
    grain (gameweek, position),
    description 'Model metrics and performance per position and gameweek'
);

SELECT
    r.gameweek,
    r.position, 
    COUNT(*)                                                        AS eligible_players,
    COUNT(CASE WHEN r.model_rank <= r.picks THEN 1 END)             AS picks_made,
    SUM(CASE WHEN r.model_rank <= r.picks THEN r.actual_points END) AS model_points,
    SUM(CASE when r.crowd_rank <= r.picks THEN r.actual_points END) AS crowd_points,
    AVG(r.actual_points)                                            AS field_avg_points,
    CORR(r.score_rank, r.actual_rank)                               AS spearman
FROM backtest.player_gw_results AS r 
GROUP BY r.gameweek, r.position