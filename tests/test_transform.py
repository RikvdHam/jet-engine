from jet_engine.infra.core.config import settings
from jet_engine.infra.db.models import ViewORM


def test_transform_success(client, uploaded_dataset, journal_mapping, tmp_storage):
    dataset_id = uploaded_dataset["dataset_id"]

    # Make sure mapping exists
    client.post(f"/api/datasets/{dataset_id}/save-mapping", json={"mapping": journal_mapping})

    # Run validation
    _ = client.get(f"/api/datasets/{dataset_id}/validate")

    # Run transform
    response = client.get(f"/api/datasets/{dataset_id}/transform")

    assert response.status_code == 200

    parquet_file = tmp_storage["validated"] / f"{dataset_id}.parquet"

    assert parquet_file.exists()


def test_transform_view_written(client, db_session, uploaded_dataset, journal_mapping):
    dataset_id = uploaded_dataset["dataset_id"]

    # Make sure mapping exists
    client.post(f"/api/datasets/{dataset_id}/save-mapping", json={"mapping": journal_mapping})

    # Run validation
    _ = client.get(f"/api/datasets/{dataset_id}/validate")

    # Run transform
    _ = client.get(f"/api/datasets/{dataset_id}/transform")

    viewORM = db_session.get(ViewORM, dataset_id)

    assert viewORM is not None
    assert viewORM.dataset_id == dataset_id
    assert viewORM.measures_json == []
    assert viewORM.dimensions_json == []
    assert viewORM.sorting_json is None
    assert viewORM.filters_json is None


def test_transform_reject_no_data(client, uploaded_dataset):
    dataset_id = uploaded_dataset["dataset_id"]

    response = client.get(f"/api/datasets/fake-id/transform")

    assert response.status_code == 404

    response = client.get(f"/api/datasets/{dataset_id}/transform")

    assert response.status_code == 404
