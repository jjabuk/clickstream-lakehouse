"""Fail if a DAG in airflow/dags does not import (ADR 0015). Runs inside the Airflow image.

DagBag parses the files as the dag processor does, Cosmos's `dbt ls` included, with no Airflow
services or metadata database.
"""

import sys

from airflow.dag_processing.dagbag import DagBag

bag = DagBag("/opt/project/airflow/dags")
for path, error in bag.import_errors.items():
    print(f"{path}:\n{error}", file=sys.stderr)
print(f"{len(bag.dags)} DAGs: {', '.join(sorted(bag.dags))}")
sys.exit(1 if bag.import_errors or not bag.dags else 0)
