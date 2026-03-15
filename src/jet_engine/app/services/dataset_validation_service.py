import os
from typing import Dict
from pathlib import Path

import polars as pl
from sqlalchemy.orm import Session
from fastapi import HTTPException

from jet_engine.infra.db.models import Dataset, DatasetMapping
from jet_engine.infra.core.config import settings
from jet_engine.domain.models import Field
from jet_engine.infra.core import field_registry


BASE_DIR = Path(__file__).resolve().parent.parent


def validate_dataset(db: Session, dataset_id: str) -> Dict:
    dataset = Dataset.load(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Load mapping
    mapping_entry = DatasetMapping.load(db, dataset_id)
    if not mapping_entry:
        raise HTTPException(status_code=400, detail="Mapping not found")
    mapping = mapping_entry.mapping_json

    # Load field metadata
    field_ids = list(mapping.values())
    rename_dict = {raw_col: field_registry.get_field(fid).canonical_name for raw_col, fid in mapping.items()}
    required_raw_cols = list(rename_dict.keys())

    # Lazy load raw data
    raw_path = os.path.join(BASE_DIR, settings.storage_raw_dir, dataset.stored_filename)
    if not os.path.exists(raw_path):
        raise HTTPException(status_code=404, detail="Raw file missing")

    lf = (
        pl.scan_parquet(raw_path)
        .select(required_raw_cols)
        .rename(rename_dict)
        .with_row_index("row_id")  # updated
    )

    # Build error expressions
    errors = []

    for field in field_registry.all(field_ids):
        field_name = field.canonical_name

        if field.is_required:
            errors.append(
                pl.when(pl.col(field_name).is_null())
                .then(pl.lit(f"{field_name} is required"))
                .otherwise(None)
            )

        if field.dtype == "int":
            errors.append(
                pl.when(
                    pl.col(field_name).is_not_null() &
                    pl.col(field_name).cast(pl.Int64, strict=False).is_null()
                )
                .then(pl.lit(f"{field_name} must be integer"))
                .otherwise(None)
            )

        elif field.dtype == "float":
            errors.append(
                pl.when(
                    pl.col(field_name).is_not_null() &
                    pl.col(field_name).cast(pl.Float64, strict=False).is_null()
                )
                .then(pl.lit(f"{field_name} must be decimal number"))
                .otherwise(None)
            )

        elif field.dtype == "date":
            errors.append(
                pl.when(
                    pl.col(field_name).is_not_null() &
                    pl.col(field_name).str.strptime(pl.Date, strict=False).is_null()
                )
                .then(pl.lit(f"{field_name} invalid date"))
                .otherwise(None)
            )

    if errors:
        error_list = pl.concat_list(errors).list.drop_nulls()
        lf = lf.with_columns(
            error_messages=error_list,
            is_valid=error_list.list.len() == 0
        )
    else:
        # No errors → all rows valid
        lf = lf.with_columns(
            error_messages=pl.lit([]),  # empty list column
            is_valid=pl.lit(True)
        )

    validated_path = os.path.join(BASE_DIR, settings.storage_validated_dir, dataset.stored_filename)

    lf.sink_parquet(
        validated_path,
        compression="zstd"
    )

    summary = (
        lf.select([
            pl.len().alias("total_rows"),
            pl.col("is_valid").sum().alias("valid_rows")
        ])
        .collect()
    )

    return {
        'total_rows': summary["total_rows"][0],
        'invalid_rows': summary["total_rows"][0] - summary["valid_rows"][0]
    }
