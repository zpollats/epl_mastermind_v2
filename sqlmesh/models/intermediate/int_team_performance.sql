MODEL (
    name epl.int_team_performance,
    kind VIEW,
    description 'Team-level rolling performance from fixture results. Uses actual match scores rather than player aggregations for accuracy.'
);

-- Build a team-gameweek record from fixtures (one row per team per GW)
WITH team_fixtures AS (
    SELECT
        home_team_id    AS team_id,
        gameweek,
        home_goals      AS goals_for,
        away_goals      AS goals_against,
        home_fdr        AS fdr,
        'home'          AS venue,
        result,
        CASE result
            WHEN 'home_win' THEN 3
            WHEN 'draw' THEN 1
            ELSE 0
        END             AS match_points
    FROM epl.stg_fixtures
    WHERE finished = true

    UNION ALL

    SELECT
        away_team_id    AS team_id,
        gameweek,
        away_goals      AS goals_for,
        home_goals      AS goals_against,
        away_fdr        AS fdr,
        'away'          AS venue,
        result,
        CASE result
            WHEN 'away_win' THEN 3
            WHEN 'draw' THEN 1
            ELSE 0
        END             AS match_points
    FROM epl.stg_fixtures
    WHERE finished = true
),

rolling AS (
    SELECT
        team_id,
        gameweek,
        goals_for,
        goals_against,
        venue,
        match_points,

        -- Rolling form (last 5 matches)
        AVG(match_points) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS form_5gw,

        AVG(goals_for) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS attack_strength_5gw,

        AVG(goals_against) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS defense_weakness_5gw,

        -- Clean sheet rate (last 5)
        AVG(CASE WHEN goals_against = 0 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS clean_sheet_rate_5gw,

        -- Season totals
        SUM(match_points) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS UNBOUNDED PRECEDING
        ) AS season_points,

        SUM(goals_for) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS UNBOUNDED PRECEDING
        ) AS season_goals_for,

        SUM(goals_against) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS UNBOUNDED PRECEDING
        ) AS season_goals_against

    FROM team_fixtures
)

SELECT * FROM rolling
