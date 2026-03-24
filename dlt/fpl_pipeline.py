"""
FPL API Pipeline — Ingest Fantasy Premier League data into DuckDB.

Data source: https://fantasy.premierleague.com/api/
No authentication required for public endpoints.

Endpoints ingested:
    - bootstrap-static -> players, teams, gameweeks
    - fixtures         -> all 380 PL fixtures with scores, stats, difficulty
"""

import dlt
import requests
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()

FPL_BASE_URL = "https://fantasy.premierleague.com/api"


@lru_cache(maxsize=1)
def _get_bootstrap() -> dict:
    """Fetch the bootstrap-static endpoint (single large JSON payload).

    Cached so players/teams/gameweeks don't each trigger a separate HTTP call.
    """
    resp = requests.get(f"{FPL_BASE_URL}/bootstrap-static/", timeout=30)
    resp.raise_for_status()
    return resp.json()


@dlt.source
def fpl_source():
    """FPL API source — extracts players, teams, gameweeks, and fixtures."""

    @dlt.resource(write_disposition="replace", max_table_nesting=0)
    def players():
        """All ~700 PL players with stats, price, ownership, ICT index."""
        yield _get_bootstrap()["elements"]

    @dlt.resource(write_disposition="replace", max_table_nesting=0)
    def teams():
        """20 PL teams with strength ratings (attack/defence, home/away)."""
        yield _get_bootstrap()["teams"]

    @dlt.resource(write_disposition="replace", max_table_nesting=0)
    def gameweeks():
        """38 gameweeks with deadlines, averages, chip usage, top scores."""
        yield _get_bootstrap()["events"]

    @dlt.resource(write_disposition="replace", max_table_nesting=0)
    def fixtures():
        """All 380 fixtures with scores, FDR, kickoff times, and match stats."""
        resp = requests.get(f"{FPL_BASE_URL}/fixtures/", timeout=30)
        resp.raise_for_status()
        yield resp.json()

    return players, teams, gameweeks, fixtures


def main():
    pipeline = dlt.pipeline(
        pipeline_name="fpl_pipeline",
        destination="duckdb",
        dataset_name="raw_fpl",
        progress="log",
    )

    load_info = pipeline.run(
        fpl_source(),
        loader_file_format="parquet",
    )
    print(load_info)


if __name__ == "__main__":
    main()