"""
football-data.org Pipeline — Ingest Premier League match data into DucLake.

Data source: https://api.football-data.org/v4/
Requires free API key (10 requests/min on free tier).

Endpoints ingested:
    - competitions/PL/matches    -> all PL matches with scores, referees, matchday
    - competitions/PL/standings  -> current league table (TOTAL, HOME, AWAY)
    - competitions/PL/scorers    -> top scorers list
"""

import os
import dlt
import requests
from dotenv import load_dotenv

load_dotenv()

FOOTBALL_DATA_BASE_URL = "https://api.football-data.org/v4"


def _get_headers() -> dict:
    """Build auth headers for football-data.org."""
    api_key = os.environ.get("FOOTBALL_DATA_API_KEY", "")
    if not api_key:
        raise ValueError(
            "FOOTBALL_DATA_API_KEY not set. "
            "Get a free key at https://www.football-data.org/client/register"
        )
    return {"X-Auth-Token": api_key}


@dlt.source
def football_data_source():
    """football-data.org source — PL matches, standings, and top scorers."""

    headers = _get_headers()

    @dlt.resource(write_disposition="replace", max_table_nesting=0)
    def pl_matches():
        """All Premier League matches for the current season."""
        resp = requests.get(
            f"{FOOTBALL_DATA_BASE_URL}/competitions/PL/matches",
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        yield resp.json()["matches"]

    @dlt.resource(write_disposition="replace", max_table_nesting=0)
    def pl_standings():
        """League standings — flattened with standing type (TOTAL, HOME, AWAY)."""
        resp = requests.get(
            f"{FOOTBALL_DATA_BASE_URL}/competitions/PL/standings",
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        for standing in resp.json()["standings"]:
            for entry in standing["table"]:
                entry["standing_type"] = standing["type"]
                yield entry

    @dlt.resource(write_disposition="replace", max_table_nesting=0)
    def pl_scorers():
        """Top scorers in the Premier League."""
        resp = requests.get(
            f"{FOOTBALL_DATA_BASE_URL}/competitions/PL/scorers",
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        yield resp.json()["scorers"]

    return pl_matches, pl_standings, pl_scorers


def main():
    pipeline = dlt.pipeline(
        pipeline_name="football_data_pipeline",
        destination="ducklake",
        dataset_name="football_data",
        progress="log",
    )

    load_info = pipeline.run(
        football_data_source(),
        loader_file_format="parquet",
    )
    print(load_info)


if __name__ == "__main__":
    main()
