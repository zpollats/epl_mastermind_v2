MODEL (
    name epl.seed_team_mapping_26_27,
    kind SEED (
        path '../../seeds/seed_team_mapping_26_27.csv'
    ),
    grain (fpl_team_id),
    description 'Mapping between FPL team IDs/names and football-data.org team names for 2026/2027.'
);
