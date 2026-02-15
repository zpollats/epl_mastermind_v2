MODEL (
    name epl.seed_team_mapping,
    kind SEED (
        path '../seeds/seed_team_mapping.csv'
    ),
    grain (fpl_team_id),
    description 'Mapping between FPL team IDs/names and football-data.org team names.'
);
