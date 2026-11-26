"""Smoke test of the Airflow stack: the scheduler can run the project's commands.

Contacts no outside service and writes nothing. Checks that a task sees the
project code and its venv, the variables, the pipeline mode, the warehouse
directory, the landing zone (read access; write access too in mode "own") and
the dbt project. No schedule: trigger it by hand from the UI.
"""

from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT_HOME = "/opt/project"
# The project's own venv, not Airflow's: see the Dockerfile.
PROJECT_BIN = "/opt/project-venv/bin"

with DAG(
    dag_id="smoke_environment",
    description="Local checks of the stack; no outside service",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["smoke"],
    doc_md=__doc__,
):
    check_environment = BashOperator(
        task_id="check_environment",
        bash_command=f"{PROJECT_BIN}/python -m ingestion.check_environment",
        cwd=PROJECT_HOME,
    )
    # Renders the profile and parses the project; dbt connects only to run something.
    dbt_parse = BashOperator(
        task_id="dbt_parse",
        bash_command=f"{PROJECT_BIN}/dbt parse",
        cwd=PROJECT_HOME,
    )

    check_environment >> dbt_parse
