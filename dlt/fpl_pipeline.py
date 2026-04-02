"""
FPL API Pipeline — Ingest Fantasy Premier League data into DuckDB.

Data source: https://fantasy.premierleague.com/api/
No authentication required for public endpoints.

Endpoints ingested:
    - bootstrap-static -> players, teams, gameweeks
    - fixtures         -> all 380 PL fixtures with scores, stats, difficulty
    - element-summary  -> per-player gameweek history (points, xG, xA, etc.)
"""

import time
import dlt
import requests
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()

FPL_BASE_URL = "https://fantasy.premierleague.com/api"

# Rate limiting config
REQUEST_DELAY = 1.0
MAX_RETRIES = 5
BACKOFF_BASE = 2.0


@lru_cache(maxsize=1)
def _get_bootstrap() -> dict:
    """Fetch the bootstrap-static endpoint (single large JSON payload).

    Cached so players/teams/gameweeks don't each trigger a separate HTTP call.
    """
    resp = requests.get(f"{FPL_BASE_URL}/bootstrap-static/", timeout=30)
    resp.raise_for_status()
    return resp.json()


def _fetch_with_retry(url: str) -> requests.Response:
    """Fetch a URL with exponential backoff on 429 errors."""
    for attempt in range(MAX_RETRIES):
        resp = requests.get(url, timeout=30)

        if resp.status_code == 429:
            wait_time = BACKOFF_BASE ** attempt
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                wait_time = max(wait_time, float(retry_after))
            print(f"  Rate limited. Waiting {wait_time:.0f}s (attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait_time)
            continue
    
        resp.raise_for_status()
        return resp
    
    raise Exception(f"Failed after {MAX_RETRIES} retries: {url}")


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


@dlt.source
def fpl_player_history_source():
    """FPL element-summary source - per-player gameweek history for backtesting."""

    @dlt.resource(write_disposition="replace", max_table_nesting=0)
    def player_gameweeks():
        """Per-player per-gameweek stats: points, xG, xA, xGC, ICT, price, etc.
        
        Loops through all active players (~700 requests).
        Rate limited to ~1 request/second with exponential backoff on 429s.
        """
        bootstrap = _get_bootstrap()
        all_players = bootstrap["elements"]

        # filter down to active players
        active_players = [p for p in all_players if p.get("minutes", 0) > 0]
        total = len(active_players)

        print(f"Fetching gameweek history for {total} active players...")

        for i, player in enumerate(active_players):
            player_id = player["id"]
            player_name = player.get("web_name", f"ID:{player_id}")

            resp = _fetch_with_retry(f"{FPL_BASE_URL}/element-summary/{player_id}/")
            data = resp.json()

            for gw_record in data.get("history", []):
                yield gw_record

            if (i + 1) % 50 == 0:
                print(f"  Progress: {i + 1}/{total} players fetched")

            # Rate limit: wait between requests
            time.sleep(REQUEST_DELAY)

        print(f"  Done: {total} players fetched")

    return player_gameweeks,


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


def load_player_history():
    pipeline = dlt.pipeline(
        pipeline_name="fpl_pipeline",
        destination="duckdb",
        dataset_name="raw_fpl",
        progress="log",
    )

    load_info = pipeline.run(
        fpl_player_history_source(),
        loader_file_format="parquet",
    )
    print(load_info)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "history":
        load_player_history()
    else:
        main()