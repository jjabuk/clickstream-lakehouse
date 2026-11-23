# Airflow with Cosmos (ADR 0017). Two Python environments, so neither pins the other:
# - Airflow's own, with Cosmos, under Airflow's constraints file;
# - /opt/project-venv, the project's locked dependencies (generator, ingest, dbt) from uv.lock.
# The repository itself is mounted at /opt/project at run time, not copied, so a code change
# needs no rebuild; only pyproject.toml and uv.lock are baked in.
FROM apache/airflow:3.4.0-python3.12

ARG AIRFLOW_VERSION=3.4.0
ARG PYTHON_VERSION=3.12
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "astronomer-cosmos==1.16.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /usr/local/bin/uv

USER root
RUN mkdir -p /opt/project-venv && chown airflow: /opt/project-venv
# dbt v2 uses a system libduckdb when there is one; without it, it would download DuckDB 1.5.4,
# which cannot open the warehouse the host writes with DuckDB 2.0 (ADR 0024).
COPY infra/duckdb/install_libduckdb.sh /tmp/install_libduckdb.sh
RUN /tmp/install_libduckdb.sh && rm /tmp/install_libduckdb.sh
USER airflow

COPY --chown=airflow: pyproject.toml uv.lock /tmp/project/
RUN cd /tmp/project \
    && UV_PROJECT_ENVIRONMENT=/opt/project-venv UV_PYTHON=/usr/local/bin/python \
       UV_PYTHON_DOWNLOADS=never uv sync --frozen --no-dev --no-install-project --no-cache \
    && rm -rf /tmp/project

# dbt-duckdb installs its extensions on first connect; fetch them at build time instead.
RUN /opt/project-venv/bin/python -c \
    "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('iceberg', 'httpfs', 'postgres')]"
