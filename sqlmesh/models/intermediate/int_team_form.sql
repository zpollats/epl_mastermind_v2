MODEL (
    name epl.int_team_form,
    kind VIEW,
    grain (team_id, fixture_id),
    description 'Team-level rolling performance from fixture results. One row per team per fixture. Venue columns describe form at that venue only. Consumers must join on earlier fixtures to avoid leakage.'
);

-- Step 1: unpivot fixtures to one row per team. Only perspective-dependent columns here.
WITH team_fixtures AS (
    SELECT
        f.fixture_id,
        f.gameweek,
        f.kickoff_time,
        f.home_team_id    AS team_id,
        f.away_team_id    AS opponent_id,
        'home'            AS venue,
        f.home_goals      AS goals_for,
        f.away_goals      AS goals_against
    FROM epl.stg_fixtures f
    WHERE f.finished = true

    UNION ALL

    SELECT
        f.fixture_id,
        f.gameweek,
        f.kickoff_time,
        f.away_team_id    AS team_id,
        f.home_team_id    AS opponent_id,
        'away'            AS venue,
        f.away_goals      AS goals_for,
        f.home_goals      AS goals_against
    FROM epl.stg_fixtures f
    WHERE f.finished = true
),

-- Step 2: derive per-match values once now that every row is from one team's perspective
match_values AS (
    SELECT
        tf.fixture_id,
        tf.gameweek,
        tf.kickoff_time,
        tf.team_id, 
        tf.opponent_id, 
        tf.venue,
        tf.goals_for,
        tf.goals_against,
        CASE 
            WHEN tf.goals_for > tf.goals_against THEN 3
            WHEN tf.goals_for = tf.goals_against THEN 1
            ELSE 0
        END AS match_points,
        CASE WHEN tf.goals_against = 0 THEN 1.0 ELSE 0.0 END AS is_clean_sheet
    FROM team_fixtures tf
)

-- Step 3: windows for rollings stats
SELECT
    mv.team_id, 
    mv.fixture_id,
    mv.gameweek,
    mv.kickoff_time,
    mv.opponent_id,
    mv.venue,
    mv.goals_for,
    mv.goals_against,
    mv.match_points,

    -- Counts (so consumers can drop warm-up rows)
    COUNT(*) OVER season        AS fixtures_played,
    COUNT(*) OVER venue_season  AS venue_fixtures_played,

    -- Overall form, last 5 matches
    AVG(mv.match_points) OVER last5 AS points_avg_last5,
    AVG(mv.goals_for) OVER last5 AS goals_for_avg_last5,
    AVG(mv.goals_against) OVER last5 AS goals_against_avg_last5,
    AVG(mv.is_clean_sheet) OVER last5 AS cs_rate_last5,

    -- Venue form, last 3 matches at venue
    AVG(mv.goals_for) OVER venue_last3 AS venue_goals_for_avg_last3,
    AVG(mv.goals_against) OVER venue_last3 AS venue_goals_against_avg_last3,
    AVG(mv.is_clean_sheet) OVER venue_last3 AS venue_cs_rate_last3,

    -- Season to date
    SUM(mv.match_points) OVER season AS season_points,
    SUM(mv.goals_for) OVER season AS season_goals_for,
    SUM(mv.goals_against) OVER season AS season_goals_against

FROM match_values mv
WINDOW 
    last5 AS (PARTITION BY mv.team_id ORDER BY mv.kickoff_time, mv.fixture_id
              ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
    venue_last3 AS (PARTITION BY mv.team_id, mv.venue ORDER BY mv.kickoff_time, mv.fixture_id
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
    season AS (PARTITION BY mv.team_id ORDER BY mv.kickoff_time, mv.fixture_id
               ROWS UNBOUNDED PRECEDING),
    venue_season AS (PARTITION BY mv.team_id, mv.venue ORDER BY mv.kickoff_time, mv.fixture_id
                     ROWS UNBOUNDED PRECEDING)