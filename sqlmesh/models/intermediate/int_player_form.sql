MODEL (
    name epl.int_player_form,
    kind VIEW,
    grain (player_id, fixture_id),
    description 'Player form through each finished fixture (inclusive). One row per player per fixture including 0-minute rows. team_id is the club the player played for in that fixture. Consumers must join on earlier fixtures to avoid leakage.'
);

-- Step 1: one row per player per fixture
WITH player_fixtures AS (
    SELECT
        pg.player_id,
        pg.fixture_id,
        f.gameweek,             -- from the fixture, same source as int_team_form
        f.kickoff_time,
        CASE WHEN pg.was_home THEN f.home_team_id ELSE f.away_team_id END AS team_id,
        pg.opponent_team_id,
        pg.was_home,
        p.player_name,
        p.position,
        pg.total_points,
        pg.minutes,
        pg.starts,
        pg.xgi,
        pg.xgc,
        pg.bonus,
        pg.ict_index,
        pg.defensive_contribution,
        pg.yellow_cards,
        pg.red_cards,
        pg.price,              -- point in time
        pg.selected_by         -- point in time; backtest's ownership baseline
    FROM epl.stg_player_gameweeks pg
    INNER JOIN epl.stg_fixtures AS f ON pg.fixture_id = f.fixture_id
    INNER JOIN epl.stg_players AS p ON pg.player_id = p.player_id
    WHERE f.finished = true
)

-- Step 2: windows for rolling stats
SELECT
    pf.player_id,
    pf.fixture_id,
    pf.gameweek,
    pf.kickoff_time,
    pf.team_id,
    pf.opponent_team_id,
    pf.was_home,
    pf.player_name,
    pf.position,
    pf.total_points,
    pf.minutes,
    pf.price,
    pf.selected_by,

    -- Counts
    COUNT(*) OVER season AS fixtures_played,
    SUM(CASE WHEN pf.minutes > 0 THEN 1 ELSE 0 END) OVER season AS appearances,

    -- Points form (owner's view: 0-minute rows count as 0)
    AVG(pf.total_points) OVER last3 AS points_avg_last3,
    AVG(pf.total_points) OVER last5 AS points_avg_last5,

    -- Rotation risk
    AVG(pf.minutes) OVER last3 AS minutes_avg_last3,
    SUM(pf.starts) OVER last5 AS starts_last5,

    -- Underlying stats
    AVG(pf.xgi) OVER last3 AS xgi_avg_last3,
    AVG(pf.xgc) OVER last3 AS xgc_avg_last3,
    AVG(pf.ict_index) OVER last3 AS ict_avg_last3,
    AVG(pf.defensive_contribution) OVER last3 AS defcon_avg_last3,
    AVG(pf.bonus) OVER last5 as bonus_avg_last5,

    -- Discipline
    SUM(pf.yellow_cards) OVER last5 AS yellows_last5,
    SUM(pf.red_cards) OVER last5 AS reds_last5,

    -- Season to date
    SUM(pf.total_points) OVER season AS season_points

FROM player_fixtures AS pf
WINDOW 
    last3 AS (PARTITION BY pf.player_id ORDER BY pf.kickoff_time, pf.fixture_id
              ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
    last5 AS (PARTITION BY pf.player_id ORDER BY pf.kickoff_time, pf.fixture_id
              ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
    season AS (PARTITION BY pf.player_id ORDER BY pf.kickoff_time, pf.fixture_id
              ROWS UNBOUNDED PRECEDING)