"""Validate that Airflow DAGs import without errors."""

from __future__ import annotations

from airflow.models.dagbag import DagBag


def main() -> None:
    bag = DagBag(
        dag_folder="/opt/airflow/dags",
        include_examples=False,
        safe_mode=False,
    )

    if bag.import_errors:
        for path, err in bag.import_errors.items():
            print(f"IMPORT ERROR in {path}:")
            print(err)
            print()
        raise SystemExit(1)

    dag_ids = ", ".join(sorted(bag.dags)) or "(none)"
    print(f"Loaded {len(bag.dags)} DAG(s): {dag_ids}")


if __name__ == "__main__":
    main()
