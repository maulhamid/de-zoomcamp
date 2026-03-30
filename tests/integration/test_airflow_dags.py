from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

from airflow.models import DagBag


def test_airflow_dags_load_without_import_errors():
    dag_bag = DagBag(dag_folder=str(Path("orchestration/dags")), include_examples=False)
    assert dag_bag.import_errors == {}
    assert "tfl_cycle_backfill" in dag_bag.dags
    assert "tfl_cycle_incremental" in dag_bag.dags

