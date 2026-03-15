import os
import uuid
import hashlib
import polars as pl
from typing import Dict
from pathlib import Path
from fastapi import HTTPException

from jet_engine.infra.core.config import settings
from jet_engine.infra.db.models import Dataset, SignatureMapping


# --------- Helper functions ---------
def compute_file_hash(file_path: str) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


async def process_csv_upload(
    company_name,
    fiscal_year,
    file,
    current_user_id,
    db
) -> Dict:

    dataset_id = str(uuid.uuid4())
    file_name = f"{dataset_id}.csv"
    file_name_parquet = f"{dataset_id}.parquet"

    tmp_file_path = Path(settings.storage_tmp_dir) / file_name
    raw_file_path = Path(settings.storage_raw_dir) / file_name_parquet

    tmp_file_path.parent.mkdir(parents=True, exist_ok=True)
    raw_file_path.parent.mkdir(parents=True, exist_ok=True)

    # ---------- 1. Stream to disk temporarily ----------
    with open(tmp_file_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            f.write(chunk)

    # ---------- 2. Convert CSV -> Parquet ----------
    try:
        df = pl.read_csv(tmp_file_path)
        df.write_parquet(raw_file_path)
    except pl.exceptions.NoDataError:
        raise HTTPException(status_code=400, detail="CSV file is empty")
    finally:
        tmp_file_path.unlink(missing_ok=True)

    # ---------- 3. Compute row count & signature ----------
    row_count = df.height
    columns = df.columns
    signature = hashlib.sha256(",".join(sorted(columns)).encode()).hexdigest()
    data_hash = compute_file_hash(str(raw_file_path))

    # ---------- 4. Save Dataset metadata ----------
    try:
        dataset = Dataset(
            id=dataset_id,
            company_name=company_name,
            fiscal_year=fiscal_year,
            stored_filename=file_name_parquet,
            uploaded_by_id=current_user_id,
            original_filename=file.filename,
            signature=signature,
            row_count=row_count,
            data_hash=data_hash
        )
        db.add(dataset)
        db.commit()

    except Exception as e:
        if os.path.exists(tmp_file_path):
            tmp_file_path.unlink(missing_ok=True)
        if os.path.exists(raw_file_path):
            raw_file_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=str(e))

    # ---------- 5. Search for suggested mapping ----------
    suggested_mapping = SignatureMapping.load_mapping(db, signature)

    return {
        "dataset_id": dataset_id,
        "columns": columns,
        "preview": df.head(50).to_dicts(),
        "suggested_mapping": suggested_mapping
    }
