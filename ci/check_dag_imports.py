"""Load every DAG the way the Airflow scheduler does; fail on import errors or missing DAGs.

Usage: python ci/check_dag_imports.py <dag_folder>
The plugins folder must be on PYTHONPATH (the DAGs import fintech_el).
"""
import sys

from airflow.models import DagBag

EXPECTED_DAGS = {"extract_incremental_loads", "extract_full_loads", "generate_daily_data"}

dag_folder = sys.argv[1] if len(sys.argv) > 1 else "airflow/dags"
dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)

failed = False

for file_path, error in dag_bag.import_errors.items():
    print(f"IMPORT ERROR in {file_path}:\n{error}")
    failed = True

missing = EXPECTED_DAGS - set(dag_bag.dag_ids)
if missing:
    print(f"MISSING DAGS: {sorted(missing)}")
    failed = True

for dag_id in sorted(dag_bag.dag_ids):
    dag = dag_bag.get_dag(dag_id)
    if not dag.tasks:
        print(f"DAG {dag_id} has no tasks")
        failed = True
    print(f"{dag_id}: {len(dag.tasks)} tasks, schedule {dag.timetable.summary}")

if failed:
    sys.exit(1)
print(f"OK: {len(dag_bag.dag_ids)} DAGs loaded without errors")
