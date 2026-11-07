AUDIT (
    name no_future_features,
    dialect duckdb
);

/*
    Generic point-in-time audit. Return rows that violate it.
    Args:
        @as_of_col     - timestamp of the latest source row a feature was built from
        @decision_col  - when the pick is made (deadline_time)
    Assumes @this_model has kickoff_time (the fixture being predicted).
*/
SELECT 
    *
FROM @this_model
WHERE 
    @as_of_col >= @decision_col
    OR
    @decision_col >= kickoff_time