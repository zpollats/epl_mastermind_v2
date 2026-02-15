MODEL (
    name epl.int_fixture_difficulty,
    kind VIEW,
    description 'Upcoming fixture difficulty for each team — next N gameweeks with FDR ratings and opponent strength.'
);

-- For each team, get their upcoming fixtures with difficulty context
WITH upcoming AS (
    SELECT
        home_team_id    AS team_id,
        gameweek,
        away_team_id    AS opponent_id,
        home_fdr        AS fdr,
        'home'          AS venue
    FROM epl.stg_fixtures
    WHERE finished = false

    UNION ALL

    SELECT
        away_team_id    AS team_id,
        gameweek,
        home_team_id    AS opponent_id,
        away_fdr        AS fdr,
        'away'          AS venue
    FROM epl.stg_fixtures
    WHERE finished = false
),

-- Get latest team performance to enrich opponent context
latest_team_perf AS (
    SELECT DISTINCT ON (team_id)
        team_id,
        form_5gw,
        attack_strength_5gw,
        defense_weakness_5gw,
        clean_sheet_rate_5gw
    FROM epl.int_team_performance
    ORDER BY team_id, gameweek DESC
),

enriched AS (
    SELECT
        u.team_id,
        u.gameweek,
        u.opponent_id,
        u.fdr,
        u.venue,
        t.team_name          AS opponent_name,

        -- Opponent strength context
        opp.form_5gw                AS opponent_form,
        opp.attack_strength_5gw     AS opponent_attack,
        opp.defense_weakness_5gw    AS opponent_defense_weakness,
        opp.clean_sheet_rate_5gw    AS opponent_clean_sheet_rate,

        -- Row number for "next N fixtures" windows
        ROW_NUMBER() OVER (
            PARTITION BY u.team_id ORDER BY u.gameweek
        ) AS fixture_order

    FROM upcoming u
    LEFT JOIN epl.stg_teams t ON u.opponent_id = t.team_id
    LEFT JOIN latest_team_perf opp ON u.opponent_id = opp.team_id
)

SELECT
    *,
    -- Average FDR over next 3 and 5 fixtures
    AVG(fdr) OVER (
        PARTITION BY team_id ORDER BY fixture_order
        ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
    ) AS avg_fdr_next_3,
    AVG(fdr) OVER (
        PARTITION BY team_id ORDER BY fixture_order
        ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING
    ) AS avg_fdr_next_5
FROM enriched
