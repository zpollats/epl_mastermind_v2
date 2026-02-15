# EPL Mastermind

A portable analytics pipeline for Premier League and Fantasy Premier League data.

**Stack:** [dlt](https://dlthub.com) → [DuckDB](https://duckdb.org) → [SQLMesh](https://sqlmesh.readthedocs.io) → GitHub Actions

## What It Does

Pulls data weekly from two APIs and builds analytical models for FPL decision-making:

- **Who should I captain?** — Player form × fixture difficulty × ownership
- **Where are the differentials?** — High-form, low-ownership picks
- **Which teams have easy runs?** — Fixture ticker with FDR ratings 5 GWs out
- **Who's overpriced?** — Cost-per-point analysis across positions

## Data Sources

| Source | What | Auth |
|---|---|---|
| [FPL API](https://fantasy.premierleague.com/api/bootstrap-static/) | Players, teams, fixtures, gameweeks | None (public) |
| [football-data.org](https://www.football-data.org/) | Match results, standings, scorers | Free API key |

## Quickstart

```bash
# 1. Clone and install
git clone https://github.com/zpollats/epl_mastermind.git
cd epl_mastermind
uv sync

# 2. Configure
cp .env.example .env
# Add your football-data.org API key to .env

# 3. Run the full pipeline
make pipeline
```

## Project Structure

```
epl_mastermind/
├── dlt/                        # Ingestion pipelines
│   ├── fpl_pipeline.py         # FPL API → DuckDB
│   └── football_data_pipeline.py  # football-data.org → DuckDB
├── sqlmesh/                    # Transformation layer
│   ├── config.yaml
│   ├── seeds/                  # Static reference data (team mappings)
│   └── models/
│       ├── staging/            # Clean & type-cast raw data
│       ├── intermediate/       # Team performance, fixture difficulty
│       └── marts/              # Final analytical models
├── data/                       # Local DuckDB database (gitignored)
├── .github/workflows/          # Weekly automated pipeline
├── Makefile                    # Common commands
└── pyproject.toml              # Dependencies (managed by uv)
```

## Make Commands

| Command | What |
|---|---|
| `make sync` | Install dependencies |
| `make ingest-fpl` | Pull FPL API data |
| `make ingest-football-data` | Pull football-data.org data |
| `make ingest-all` | Run both ingestion pipelines |
| `make sqlmesh-plan-dev` | Plan & run models in dev |
| `make sqlmesh-plan` | Plan & run models in prod |
| `make sqlmesh-run` | Run scheduled model refresh |
| `make sqlmesh-ui` | Launch SQLMesh browser UI |
| `make pipeline` | Full pipeline: ingest + transform |
| `make clean` | Delete local data files |

## Model Lineage

```
raw_fpl.players ──────┐
raw_fpl.teams ────────┤
raw_fpl.fixtures ─────┼──▶ stg_players     ──┐
seed_team_mapping ────┤    stg_teams        ──┤
                      │    stg_fixtures     ──┼──▶ int_team_performance  ──┐
                      │                       │    int_fixture_difficulty ──┼──▶ mart_player_picks
                      │                       │                            │    mart_fixture_ticker
raw_football_data     │                       │                            │
  .matches ───────────┴──▶ stg_matches      ──┘                           │
```

## Roadmap

- [ ] **Phase 1:** Core pipeline (current) — dlt + SQLMesh + DuckDB locally
- [ ] **Phase 2:** Serving layer — Evidence.dev or Streamlit dashboard
- [ ] **Phase 3:** Cloud storage — Cloudflare R2 for Parquet persistence
- [ ] **Phase 4:** ELO rating system — Python/Polars model in SQLMesh
- [ ] **Phase 5:** ML predictions — Port baseline model with opponent strength features
