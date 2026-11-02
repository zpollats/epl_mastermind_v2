MODEL (
    name epl.int_fixture_difficulty,
    kind VIEW,
    grain (team_id, fixture_id),
    description 'Every fixture (past and upcoming) from one team''s perspective with own and opponent form as of that gameweek''s deadline.'
);

-- For each team, get past and upcoming fixtures
WITH team_fixtures AS (
    SELECT
        f.fixture_id,
        f.gameweek,
        f.kickoff_time,
        f.finished,
        f.home_team_id AS team_id,
        f.away_team_id AS opponent_id,
        'home'         AS venue,
        'away'         AS opponent_venue,
        f.home_fdr     AS fdr
    FROM epl.stg_fixtures f

    UNION ALL

    SELECT
        f.fixture_id,
        f.gameweek,
        f.kickoff_time,
        f.finished,
        f.away_team_id AS team_id,
        f.home_team_id AS opponent_id,
        'away'         AS venue,
        'home'         AS opponent_venue,
        f.away_fdr     AS fdr
    FROM epl.stg_fixtures f
),

-- join for the deadline time
with_deadline AS (
    SELECT
        tf.fixture_id, 
        tf.gameweek,
        tf.kickoff_time, 
        tf.finished,
        tf.team_id,
        tf.opponent_id,
        tf.venue,
        tf.opponent_venue,
        tf.fdr,
        gw.deadline_time
    FROM team_fixtures AS tf
    INNER JOIN epl.stg_gameweeks AS gw ON tf.gameweek = gw.gameweek
),

-- Get latest teams performances from int_team_form to enrich data
enriched AS (
    SELECT
        wd.team_id,
        wd.fixture_id,
        wd.gameweek,
        wd.kickoff_time,
        wd.deadline_time,
        wd.finished,
        wd.opponent_id,
        wd.venue,
        wd.fdr,
        t.team_name AS opponent_name,

        -- own team as of deadline
        own.points_avg_last5 AS team_form,
        own.goals_for_avg_last5 AS team_attack,
        own.goals_against_avg_last5 AS team_defense_weakness,
        own.cs_rate_last5 AS team_cs_rate,

        -- opponent overall
        opp.points_avg_last5 AS opponent_form,
        opp.goals_for_avg_last5 AS opponent_attack,
        opp.goals_against_avg_last5 AS opponent_defense_weakness,
        opp.cs_rate_last5 AS opponent_clean_sheet_rate,

        -- opponent venue form
        ov.venue_goals_for_avg_last3 AS opponent_venue_attack,
        ov.venue_goals_against_avg_last3 AS opponent_venue_defense,
        ov.venue_cs_rate_last3 AS opponent_venue_cs_rate, 

        -- auditability: when the opponent form was last updated
        opp.kickoff_time AS opponent_form_as_of

    FROM with_deadline as wd
    LEFT JOIN epl.stg_teams AS t ON wd.opponent_id = t.team_id
    ASOF LEFT JOIN epl.int_team_form AS own
        ON wd.team_id = own.team_id
        AND wd.deadline_time > own.kickoff_time
    ASOF LEFT JOIN epl.int_team_form AS opp
        ON wd.opponent_id = opp.team_id
        AND wd.deadline_time > opp.kickoff_time
    ASOF LEFT JOIN epl.int_team_form AS ov
        ON wd.opponent_id = ov.team_id
        AND wd.opponent_venue = ov.venue
        AND wd.deadline_time > ov.kickoff_time
)

SELECT
    e.team_id,
    e.fixture_id,
    e.gameweek,
    e.kickoff_time,
    e.deadline_time,
    e.finished,
    e.opponent_id,
    e.venue,
    e.fdr,
    e.opponent_name,
    e.team_form,
    e.team_attack,
    e.team_defense_weakness,
    e.team_cs_rate,
    e.opponent_form,
    e.opponent_attack,
    e.opponent_defense_weakness,
    e.opponent_clean_sheet_rate,
    e.opponent_venue_attack,
    e.opponent_venue_defense,
    e.opponent_venue_cs_rate, 
    e.opponent_form_as_of,

    -- upcoming-only columns, NULL for finished rows
    CASE WHEN NOT e.finished THEN ROW_NUMBER() OVER upcoming END AS fixture_order,
    CASE WHEN NOT e.finished THEN AVG(e.fdr) OVER next3 END AS avg_fdr_next_3,
    CASE WHEN NOT e.finished THEN AVG(e.fdr) OVER next5 END AS avg_fdr_next_5

FROM enriched e
WINDOW 
    upcoming AS (PARTITION BY e.team_id, e.finished ORDER BY e.kickoff_time, e.fixture_id),
    next3 AS (PARTITION BY e.team_id, e.finished ORDER BY e.kickoff_time, e.fixture_id
              ROWS BETWEEN CURRENT ROW and 2 FOLLOWING),
    next5 AS (PARTITION BY e.team_id, e.finished ORDER BY e.kickoff_time, e.fixture_id
              ROWS BETWEEN CURRENT ROW and 4 FOLLOWING)
