import pytest
import json

from tests.utils import get_test_file


@pytest.fixture
def uploaded_dataset(client):

    with open(get_test_file("sample_upload.csv"), "rb") as f:
        response = client.post(
            "/api/uploads/csv",
            data={"company_name": "TEST_COMPANY", "fiscal_year": 2025},
            files={"file": ("sample_upload.csv", f, "text/csv")}
        )

    assert response.status_code == 200

    return response.json()


@pytest.fixture
def journal_mapping():

    with open(get_test_file("journal_mapping.json")) as f:
        return json.load(f)


def test_save_valid_mapping(client, uploaded_dataset, journal_mapping):

    dataset_id = uploaded_dataset["dataset_id"]

    response = client.post(
        f"/api/datasets/{dataset_id}/save-mapping",
        json={"mapping": journal_mapping}
    )

    assert response.status_code == 200


def test_invalid_field_mapping(client, uploaded_dataset, journal_mapping):

    dataset_id = uploaded_dataset["dataset_id"]

    journal_mapping.update({
        'revenue_column': 'fake-id'
    })

    response = client.post(
        f"/api/datasets/{dataset_id}/save-mapping",
        json={"mapping": journal_mapping}
    )

    assert response.status_code == 400


def test_duplicate_fields(client, uploaded_dataset, journal_mapping):

    dataset_id = uploaded_dataset["dataset_id"]

    journal_mapping.update({
        'test_column': list(journal_mapping.values())[0]
    })

    response = client.post(
        f"/api/datasets/{dataset_id}/save-mapping",
        json={"mapping": journal_mapping}
    )

    assert response.status_code == 400


def test_dataset_not_found(client, journal_mapping):

    response = client.post(
        "/api/datasets/unknown/save-mapping",
        json={"mapping": journal_mapping}
    )

    assert response.status_code == 404
