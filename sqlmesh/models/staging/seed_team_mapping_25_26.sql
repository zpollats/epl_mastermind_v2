MODEL (
    name epl.seed_team_mapping_25_26,
    kind SEED (
        path '../../seeds/seed_team_mapping_25_26.csv'
    ),
    grain (fpl_team_id),
    description 'Mapping between FPL team IDs/names and football-data.org team names for 2025/2026.'
);
