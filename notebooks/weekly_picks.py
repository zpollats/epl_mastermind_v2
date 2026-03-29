import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import polars as pl

    return (duckdb,)


@app.cell
def _(duckdb):
    conn = duckdb.connect('./data/fpl.duckdb')
    return (conn,)


@app.cell
def _(conn):
    top_picks = conn.sql("FROM epl__dev.mart_player_picks ORDER BY pick_score DESC LIMIT 10")
    return (top_picks,)


@app.cell
def _(top_picks):
    top_picks.pl()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
