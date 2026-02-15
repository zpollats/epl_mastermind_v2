MODEL (
    name epl.stg_teams,
    kind VIEW,
    description 'PL teams with FPL strength ratings and name mapping to football-data.org.'
);

SELECT
    t.id                                AS team_id,
    t.name                              AS team_name,
    t.short_name,

    -- FPL strength ratings (1-5 scale, higher = stronger)
    t.strength,
    t.strength_overall_home,
    t.strength_overall_away,
    t.strength_attack_home,
    t.strength_attack_away,
    t.strength_defence_home,
    t.strength_defence_away,

    -- Mapping to football-data.org
    m.football_data_name

FROM raw_fpl.teams t
LEFT JOIN epl.seed_team_mapping m
    ON t.id = m.fpl_team_id
