# EPL Mastermind

A portable analytics pipeline for Premier League and Fantasy Premier League data.

**Stack:** [dlt](https://dlthub.com) → [DuckDB](https://duckdb.org) → [SQLMesh](https://sqlmesh.readthedocs.io)

## What It Does

Pulls data weekly from two APIs and builds analytical models for FPL decision-making:

- **Who should I captain?** — Player form × fixture difficulty × ownership
- **Where are the differentials?** — High-form, low-ownership picks
- **Which teams have easy runs?** — Fixture ticker with FDR ratings 5 GWs out
- **Does the scoring model work?** Backtests the player-scoring model against historical gameweek results

## Data Sources

| Source | What | Auth |
|---|---|---|
| [FPL API](https://fantasy.premierleague.com/api/bootstrap-static/) | Players, teams, fixtures, gameweeks | None (public) |
| [football-data.org](https://www.football-data.org/) | Match results, standings, scorers | Free API key |

## Quickstart

```bash
# 1. Clone and install
git clone https://github.com/zpollats/epl_mastermind_v2.git
cd epl_mastermind_v2
uv sync

# 2. Configure
cp .env.example .env
# Adding your football-data.org API key to .env is optional for running

# 3. Run the full pipeline
make run
```

## Project Structure

```
epl_mastermind_v2/
├── dlt/                        # Ingestion pipelines
│   ├── fpl_pipeline.py         # FPL API → DuckDB
│   └── football_data_pipeline.py  # football-data.org → DuckDB
├── sqlmesh/                    # Transformation layer
│   ├── config.yaml
│   ├── seeds/                  # Static reference data (team mappings for 25/26 and 26/27 seasons)
│   └── models/
│       ├── staging/            # Clean & type-cast raw data
│       ├── intermediate/       # Team performance, fixture difficulty
│       └── marts/              # Final analytical models
├── data/                       # Local DuckDB database (gitignored)
├── .github/workflows/          # Weekly automated pipeline (in progress)
├── Makefile                    # Common commands
└── pyproject.toml              # Dependencies (managed by uv)
```

## Make Commands

| Command | What |
|---|---|
| `make sync` | Install dependencies |
| `make ingest-fpl` | Pull FPL API data |
| `make ingest-fd` | Pull football-data.org data |
| `make ingest-history` | Pull FPL player history data |
| `make ingest` | Run both FPL ingestion pipelines (fpl and history)|
| `make plan-dev` | Plan & run SQLMesh models in dev |
| `make plan` | Plan & run SQLMesh models in prod |
| `make run` | Full pipeline: ingest data + plan prod|
| `make test` | Run SQLMesh tests |
| `make ui` | Launch SQLMesh browser UI |
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

## Design Decisions

- **dlt** handles schema inference and JSON normalization, and can swap destinations without changing extraction code
- **SQLMesh** over dbt: plan/apply shows breaking versus non-breaking changes before anything runs, dev environments are virtual (no recompute), and unit tests and audits are built in.
- **DuckDB**: data is small (MBs), so an in-process columnar engine beats running a server, and it reads Parquet on S3 directly when project moves to clound.
- Backtests are separated because production must never depend on analysis. Both share one scoring path, so the backtest measures what actually ships.

## Roadmap

- [x] **Phase 1:** Core pipeline: dlt + SQLMesh + DuckDB, run locally
- [ ] **Phase 2:** Containerize: Docker image that runs the full pipeline; SQLMesh tests and audits for data quality
- [ ] **Phase 3:** AWS deployment: S3 storage, ECR + ECS Fargate on an EventBridge schedule, least-privilege IAM, CloudWatch alerts, all provisioned with Terraform
- [ ] **Phase 4:** CI/CD: GitHub Actions runs tests and `sqlmesh plan` on PRs, builds and pushes the image on merge
- [ ] **Phase 5:** Serving layer: public dashboard (Evidence.dev or Streamlit)
- [ ] **Phase 6:** Modeling: ELO ratings and match predictions
