import pytest
import json

from tests.utils import get_test_file


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
        'amount': 'fake-id'
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


def test_missing_mandatory_field(client, uploaded_dataset, journal_mapping):

    dataset_id = uploaded_dataset["dataset_id"]

    _ = journal_mapping.pop("account_number")

    response = client.post(
        f"/api/datasets/{dataset_id}/save-mapping",
        json={"mapping": journal_mapping}
    )

    assert response.status_code == 400


def test_invalid_debit_credit_combo(client, uploaded_dataset, journal_mapping):

    dataset_id = uploaded_dataset["dataset_id"]

    _ = journal_mapping.pop("amount")
    journal_mapping.update({
        "debit_amount": "f8c7b6d2-0c1b-4e1e-bb2f-1f7a8c5b6d4e"
    })

    response = client.post(
        f"/api/datasets/{dataset_id}/save-mapping",
        json={"mapping": journal_mapping}
    )

    assert response.status_code == 400

    journal_mapping.update({
        "amount": "97acfd5e-1c61-481a-a97a-eefd5973789e",
        "debit_amount": "f8c7b6d2-0c1b-4e1e-bb2f-1f7a8c5b6d4e",
        "credit_amount": "d7f2a9b4-6e3b-4c8b-b6d2-f7a5c2b8d9e0",
        "debit_credit_indicator": "76a53a2b-b656-4c1f-a6e3-a930da63a308"
    })

    response = client.post(
        f"/api/datasets/{dataset_id}/save-mapping",
        json={"mapping": journal_mapping}
    )

    assert response.status_code == 400
