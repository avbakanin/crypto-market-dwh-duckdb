# syntax=docker/dockerfile:1
#
# The official Airflow image plus this project. Airflow keeps its own tested
# environment (its constraints are not ours); the project runs from a separate
# venv built from the same pinned requirements as the local .venv (DuckDB and
# dbt-duckdb, no database driver, no keys), so ingestion and dbt behave exactly
# as on the host.
#
# Tags crypto-market-dwh-duckdb:*: the Snowflake original builds crypto-market-dwh:*
# on the same machine.
#
#   docker build --target runtime -t crypto-market-dwh-duckdb:dev .
#   docker build --target test -t crypto-market-dwh-duckdb:test . && docker run --rm crypto-market-dwh-duckdb:test
#   docker run --rm --entrypoint python crypto-market-dwh-duckdb:test -m pytest -q tests_airflow
#   docker build --target dashboard -t crypto-market-dwh-duckdb-dashboard:dev .

ARG AIRFLOW_IMAGE=apache/airflow:3.4.0-python3.12

FROM ${AIRFLOW_IMAGE} AS runtime

ENV PROJECT_HOME=/opt/project \
    PROJECT_VENV=/opt/project-venv

# /opt belongs to root; everything below it belongs to the airflow user, and
# pip runs as that user (the image refuses pip installs as root).
USER root
RUN mkdir -p "${PROJECT_HOME}" "${PROJECT_VENV}" \
    && chown airflow:0 "${PROJECT_HOME}" "${PROJECT_VENV}"
# LightGBM's wheel links OpenMP (libgomp), which the Airflow image lacks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Dependencies before code: this layer is rebuilt only when the pins change.
# The venv comes from the base interpreter, not from Airflow's own venv.
# dbt-core-experimental-parser is an sdist whose build step downloads its
# wheel from GitHub releases, so the build needs github.com as well as PyPI.
# The forward test's daily step (signals/) scores with pandas and LightGBM.
COPY --chown=airflow:0 requirements.txt requirements-dbt.txt requirements-signals.txt \
    /tmp/requirements/
RUN /usr/python/bin/python -m venv "${PROJECT_VENV}" \
    && "${PROJECT_VENV}/bin/pip" install --no-cache-dir --disable-pip-version-check \
        -r /tmp/requirements/requirements-dbt.txt \
        -r /tmp/requirements/requirements-signals.txt \
    && "${PROJECT_VENV}/bin/pip" check \
    && python -m pip check

COPY --chown=airflow:0 . ${PROJECT_HOME}
RUN cd "${PROJECT_HOME}" && "${PROJECT_VENV}/bin/dbt" deps --project-dir dbt


# Dashboard: Streamlit + DuckDB over the Parquet exports in data/marts. Its own small
# image on plain Python: no Airflow, no Snowflake connector, no keys, no project code
# but dashboard/.
FROM python:3.12-slim AS dashboard

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1     MARTS_DIR=/data/marts

COPY requirements-dashboard.txt /tmp/requirements/
RUN python -m pip install --no-cache-dir --disable-pip-version-check         -r /tmp/requirements/requirements-dashboard.txt     && python -m pip check     && useradd --create-home --uid 1000 dashboard

WORKDIR /app
COPY dashboard/ /app/dashboard/
USER dashboard
EXPOSE 8501
# python -m puts /app on sys.path, so `from dashboard import data` resolves.
# No usage statistics, no file watcher (the code is baked in), no browser. An explicit
# browser.serverAddress also stops the headless start-up from asking checkip.amazonaws.com
# for the machine's public IP to print an "External URL".
CMD ["python", "-m", "streamlit", "run", "dashboard/app.py",      "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true",      "--browser.serverAddress=localhost", "--browser.gatherUsageStats=false",      "--server.fileWatcherType=none"]


# Linux check of the same code: pytest, ruff and pyright on top of runtime.
FROM runtime AS test

RUN "${PROJECT_VENV}/bin/pip" install --no-cache-dir --disable-pip-version-check \
        -r "${PROJECT_HOME}/requirements-dev.txt" \
    && "${PROJECT_VENV}/bin/python" -m pyright --version

# DAG tests (tests_airflow/) need Airflow, so they run with Airflow's own
# interpreter: pytest goes into Airflow's environment in this stage only.
RUN python -m pip install --no-cache-dir --disable-pip-version-check "pytest>=8.0" \
    && python -m pip check

# pyproject.toml points pyright at ./.venv; the link makes that path the project venv.
RUN ln -s "${PROJECT_VENV}" "${PROJECT_HOME}/.venv"

WORKDIR ${PROJECT_HOME}
ENTRYPOINT ["/opt/project-venv/bin/python", "-m"]
CMD ["pytest", "-q"]
