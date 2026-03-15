import polars as pl

from jet_engine.infra.db.models import Dataset

from tests.utils import get_test_file


def test_upload_success(client, tmp_storage):

    with open(get_test_file("journal_correct.csv"), "rb") as f:
        response = client.post(
            "/api/uploads/csv",
            data={"company_name": "COMP A", "fiscal_year": 2025},
            files={"file": ("journal_correct.csv", f, "text/csv")}
        )

    assert response.status_code == 200

    response_json = response.json()

    dataset_id = response_json["dataset_id"]
    parquet_file = tmp_storage["raw"] / f"{dataset_id}.parquet"

    assert parquet_file.exists()


def test_upload_rejects_non_csv(client):

    response = client.post(
        "/api/uploads/csv",
        data={"company_name": "COMP A", "fiscal_year": 2025},
        files={"file": ("test.txt", b"hello", "text/plain")}
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Only CSV files allowed"


def test_dataset_metadata_written(client, db_session):

    with open(get_test_file("journal_correct.csv"), "rb") as f:
        response = client.post(
            "/api/uploads/csv",
            data={"company_name": "COMP A", "fiscal_year": 2025},
            files={"file": ("journal_correct.csv", f, "text/csv")}
        )

    dataset_id = response.json()["dataset_id"]

    dataset = db_session.get(Dataset, dataset_id)

    assert dataset is not None
    assert dataset.company_name == "COMP A"
    assert dataset.fiscal_year == 2025


def test_parquet_contains_data(client, tmp_storage):

    with open(get_test_file("journal_correct.csv"), "rb") as f:
        response = client.post(
            "/api/uploads/csv",
            data={"company_name": "COMP A", "fiscal_year": 2025},
            files={"file": ("journal_correct.csv", f, "text/csv")}
        )

    dataset_id = response.json()["dataset_id"]

    parquet_file = tmp_storage["raw"] / f"{dataset_id}.parquet"

    df = pl.read_parquet(parquet_file)

    assert df.height > 0


def test_empty_csv(client):

    with open(get_test_file("empty.csv"), "rb") as f:
        response = client.post(
            "/api/uploads/csv",
            data={"company_name": "COMP A", "fiscal_year": 2025},
            files={"file": ("empty.csv", f, "text/csv")}
        )

    assert response.status_code in (200, 400)


def test_tmp_file_removed(client, tmp_storage):

    with open(get_test_file("journal_correct.csv"), "rb") as f:
        response = client.post(
            "/api/uploads/csv",
            data={"company_name": "COMP A", "fiscal_year": 2025},
            files={"file": ("journal_correct.csv", f, "text/csv")}
        )

    tmp_files = list(tmp_storage["tmp"].glob("*.csv"))

    assert len(tmp_files) == 0
