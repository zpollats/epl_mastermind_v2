"""
FPL API Pipeline — Ingest Fantasy Premier League data into DucLake.

Data source: https://fantasy.premierleague.com/api/
No authentication required for public endpoints.

Endpoints ingested:
    - bootstrap-static -> players, teams, gameweeks
    - fixtures         -> all 380 PL fixtures with scores, stats, difficulty
"""

import dlt
import requests
from dotenv import load_dotenv

load_dotenv()

FPL_BASE_URL = "https://fantasy.premierleague.com/api"


@dlt.source
def fpl_source():
    """FPL API source — extracts players, teams, gameweeks, and fixtures."""

    def _get_bootstrap():
        """Fetch the bootstrap-static endpoint (single large JSON payload)."""
        resp = requests.get(f"{FPL_BASE_URL}/bootstrap-static/", timeout=30)
        resp.raise_for_status()
        return resp.json()

    @dlt.resource(write_disposition="replace", max_table_nesting=0)
    def players():
        """All ~700 PL players with stats, price, ownership, ICT index."""
        data = _get_bootstrap()
        yield data["elements"]

    @dlt.resource(write_disposition="replace", max_table_nesting=0)
    def teams():
        """20 PL teams with strength ratings (attack/defence, home/away)."""
        data = _get_bootstrap()
        yield data["teams"]

    @dlt.resource(write_disposition="replace", max_table_nesting=0)
    def gameweeks():
        """38 gameweeks with deadlines, averages, chip usage, top scores."""
        data = _get_bootstrap()
        yield data["events"]

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
        destination="ducklake",
        dataset_name="fpl",
        progress="log",
    )

    load_info = pipeline.run(
        fpl_source(),
        loader_file_format="parquet",
    )
    print(load_info)


if __name__ == "__main__":
    main()
