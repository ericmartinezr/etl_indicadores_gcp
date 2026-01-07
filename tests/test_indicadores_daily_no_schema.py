import pytest
from airflow.models import DagBag
from airflow import DAG


PROJECT_ID = "etl-indicadores"
DATASET_ID = "ds_indicadores"
TABLE_ID = "tbl_indicadores"


@pytest.fixture()
def dagbag() -> DagBag:
    return DagBag()


@pytest.fixture()
def dag(dagbag: DagBag) -> (DAG | None):
    return dagbag.get_dag(dag_id="indicadores_no_schema")


def test_dag(dag, dagbag):
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 7

    for dag_id, dag in dagbag.dags.items():
        for task in dag.tasks:
            assert task.retries >= 1, f"Task {task.task_id} in {dag_id} must have retries >= 1"


def assert_dag_dict_equal(source, dag):
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)


def test_dag_structure(dag):
    assert_dag_dict_equal(
        {
            "extract": ["transform"],
            "transform": ["save-to-gcs"],
            "save-to-gcs": ["delete-from-table"],
            "delete-from-table": ["insert-to-table"],
            "insert-to-table": ["check-table"],
            "check-table": ["delete-file"],
            "delete-file": [],
        },
        dag,
    )
