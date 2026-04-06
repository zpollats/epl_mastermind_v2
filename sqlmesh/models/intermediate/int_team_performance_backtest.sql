MODEL (
    name epl.int_team_performance_backtest,
    kind VIEW,
    description 'Team-level rolling performance lagged one gameweek for backtesting. Uses actual match scores rather than player aggregations for accuracy.'
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

-- all matches regardless of home/away
blended AS (
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
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS form_5gw,

        AVG(goals_for) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS attack_strength_5gw,

        AVG(goals_against) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS defense_weakness_5gw,

        -- Clean sheet rate (last 5)
        AVG(CASE WHEN goals_against = 0 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS clean_sheet_rate_5gw,

        -- Season totals
        SUM(match_points) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_points,

        SUM(goals_for) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_goals_for,

        SUM(goals_against) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS season_goals_against

    FROM team_fixtures
),

-- Home-only rolling stats (last 3 home matches)
home_rolling AS (
    SELECT
        team_id,
        gameweek,
        AVG(goals_for) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS home_attack_3gw,

        AVG(goals_against) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS home_defense_3gw,

        AVG(CASE WHEN goals_against = 0 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS home_cs_rate_3gw
    
    FROM team_fixtures
    WHERE venue = 'home'
),

-- Away-only rolling stats (last 3 away matches)
away_rolling AS (
    SELECT
        team_id,
        gameweek,
        AVG(goals_for) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS away_attack_3gw,

        AVG(goals_against) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS away_defense_3gw,

        AVG(CASE WHEN goals_against = 0 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY team_id ORDER BY gameweek
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS away_cs_rate_3gw
    
    FROM team_fixtures
    WHERE venue = 'away'
)

SELECT 
    b.*,

    -- Home form 
    h.home_attack_3gw,
    h.home_defense_3gw,
    h.home_cs_rate_3gw,

    -- Away form
    a.away_attack_3gw,
    a.away_defense_3gw,
    a.away_cs_rate_3gw

FROM blended b
LEFT JOIN home_rolling h 
    ON b.team_id = h.team_id 
    AND b.gameweek = h.gameweek
    AND b.venue = 'home'
LEFT JOIN away_rolling a 
    ON b.team_id = a.team_id 
    AND b.gameweek = a.gameweek
    AND b.venue = 'away'
    