# import os
# from pathlib import Path
#
# from jet_engine.infra.core.config import settings
#
#
# def test_validate_happy_path(client, uploaded_dataset, journal_mapping):
#     dataset_id = uploaded_dataset["dataset_id"]
#
#     # Make sure mapping exists
#     client.post(f"/api/datasets/{dataset_id}/save-mapping", json={"mapping": journal_mapping})
#
#     # Run validation
#     response = client.get(f"/api/datasets/{dataset_id}/validate")
#
#     assert response.status_code == 200
#     data = response.json()
#
#     # Should match number of rows in sample CSV
#     assert data["total_rows"] > 0
#     assert data["invalid_rows"] == 0  # everything valid in a correct CSV
#
#     # Check validated Parquet file exists
#     validated_path = Path(settings.storage_validated_dir) / f"{dataset_id}.parquet"
#     assert validated_path.exists()
#
#
# def test_validate_no_mapping(client, uploaded_dataset):
#     dataset_id = uploaded_dataset["dataset_id"]
#
#     # Do NOT save mapping
#     response = client.get(f"/api/datasets/{dataset_id}/validate")
#     assert response.status_code == 400
#
#
# def test_validate_no_raw_file(client, uploaded_dataset, journal_mapping):
#     dataset_id = uploaded_dataset["dataset_id"]
#
#     # Save mapping
#     client.post(f"/api/datasets/{dataset_id}/save-mapping", json={"mapping": journal_mapping})
#
#     # Delete raw parquet to simulate missing file
#     raw_file = os.path.join(settings.storage_raw_dir, uploaded_dataset["dataset_id"] + ".parquet")
#     if os.path.exists(raw_file):
#         os.remove(raw_file)
#
#     response = client.get(f"/api/datasets/{dataset_id}/validate")
#     assert response.status_code == 404
#
#
# def test_validate_invalid_rows(client, uploaded_invalid_dataset, journal_mapping):
#     dataset_id = uploaded_invalid_dataset["dataset_id"]
#
#     # Save mapping
#     client.post(f"/api/datasets/{dataset_id}/save-mapping", json={"mapping": journal_mapping})
#
#     # Validate
#     response = client.get(f"/api/datasets/{dataset_id}/validate")
#     data = response.json()
#     assert data["total_rows"] == 10
#     assert data["invalid_rows"] == 2  # two problematic rows
