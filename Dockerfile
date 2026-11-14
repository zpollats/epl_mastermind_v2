# Get python slim image based on current working version
FROM python:3.12-slim

# Pull specific version of uv being used; may update soon
COPY --from=ghcr.io/astral-sh/uv:0.8.8 /uv /uvx /bin/

# Set the environment variable to disable anonymous telemetry
# Force uv to pre-compile bytecode globally
# Force copy mode to avoid hardlink warnings
# Specify container location for duckdb file
ENV RUNTIME__DLTHUB_TELEMETRY=False UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy FPL_DUCKDB_PATH="/data/fpl.duckdb" \
    SQLMESH__DISABLE_ANONYMIZED_ANALYTICS=1 UV_NO_SYNC=1 \
    PYTHONUNBUFFERED=1

# install make
RUN apt-get update && \
    apt-get install -y --no-install-recommends make && \
    rm -rf /var/lib/apt/lists/*

# set the working directory
WORKDIR /app

RUN useradd -u 1001 -m -s /bin/bash appuser

# install dependencies using uv
COPY uv.lock pyproject.toml .python-version ./
RUN uv sync --locked
COPY --chown=appuser . .

RUN mkdir -p /data && chown -R appuser /data

USER appuser

ENTRYPOINT ["make"]
CMD ["run"]