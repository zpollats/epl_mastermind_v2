MODEL (
    name epl.stg_gameweeks,
    kind VIEW,
    grain (gameweek),
    description 'FPL gameweeks with deadlines. The deadline is the decision point for point-in-time features.'
);

SELECT
    id                                 AS gameweek,
    CAST(deadline_time AS TIMESTAMPTZ)  AS deadline_time,
    finished,
    is_current,
    is_next
FROM raw_fpl.gameweeks