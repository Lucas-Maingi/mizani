FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[ui]"

COPY dbt ./dbt
RUN cd dbt && dbt deps --profiles-dir .

COPY scripts ./scripts
COPY tests ./tests

ENV MIZANI_DATA_DIR=/app/data \
    MIZANI_DB=/app/data/mizani.duckdb \
    DAGSTER_HOME=/app/.dagster

RUN mkdir -p /app/data /app/.dagster

# default: run the full pipeline once (bronze -> silver -> dbt build)
CMD ["sh", "-c", "python -m mizani.bronze.run && python -m mizani.silver.run && cd dbt && dbt build --profiles-dir ."]
