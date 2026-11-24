# The stream sink (ADR 0005): the project's locked environment, nothing else. The repository is
# mounted at /opt/project at run time, as for Airflow, so a code change needs only a restart.
FROM python:3.12.16-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /tmp/project/
RUN cd /tmp/project \
    && UV_PROJECT_ENVIRONMENT=/opt/project-venv UV_PYTHON_DOWNLOADS=never \
       uv sync --frozen --no-dev --no-install-project --no-cache \
    && rm -rf /tmp/project

WORKDIR /opt/project
CMD ["/opt/project-venv/bin/python", "-m", "ingest.stream", "sink"]
