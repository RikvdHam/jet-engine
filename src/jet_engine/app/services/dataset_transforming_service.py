import os
import uuid
from pathlib import Path

import polars as pl
from sqlalchemy.orm import Session

from jet_engine.infra.db.models import ViewORM, User, Dataset
from jet_engine.infra.core.config import settings
# from jet_engine.app.services.dataset_query_service import create_view


BASE_DIR = Path(__file__).resolve().parent.parent


def has_columns(lf: pl.LazyFrame, *cols: str) -> bool:
    existing = lf.collect_schema().names()
    return all(col in existing for col in cols)


def transform_dataset(db: Session, dataset_id: str, current_user: User) -> ViewORM: #TODO: Is it?
    dataset = Dataset.load(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    data_path = os.path.join(BASE_DIR, settings.storage_validated_dir, dataset.stored_filename)
    if not os.path.exists(data_path):
        raise HTTPException(status_code=404, detail="Validated file missing")

    lf = (
        pl.scan_parquet(data_path)
        .filter(pl.col("is_valid") == True)
    )

    if has_columns(lf, "debit_amount", "credit_amount"):
        # Standardize debit/credit to amount and flag
        lf = lf.with_columns([
            # Create signed amount
            pl.when(pl.col("debit_amount").is_not_null() & (pl.col("debit_amount") != 0))
            .then(pl.col("debit_amount"))
            .when(pl.col("credit_amount").is_not_null() & (pl.col("credit_amount") != 0))
            .then(-pl.col("credit_amount"))
            .otherwise(0)
            .alias("amount"),

            # Create debit_credit_flag
            pl.when(pl.col("debit_amount") > 0)
            .then(pl.lit("D"))
            .when(pl.col("credit_amount") > 0)
            .then(pl.lit("C"))
            .otherwise(None)
            .alias("debit_credit_flag")
        ]).drop(["debit_amount", "credit_amount"])

    else :
        # Standardize debit_credit_flag to C/D/UNKNOWN
        lf = lf.with_columns([
            pl.col("debit_credit_flag")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_lowercase()
            .str.slice(0, 1)
            .alias("_dc_first")
        ])

        lf = lf.with_columns([
            pl.when(pl.col("_dc_first") == "c")
            .then("C")
            .when(pl.col("_dc_first") == "d")
            .then("D")
            .otherwise("UNKNOWN")
            .alias("debit_credit_flag")
        ]).drop("_dc_first")

    # Credit negative values and debit positive values
    lf = lf.with_columns([
        pl.when(pl.col("debit_credit_flag") == "C")
        .then(-pl.col("amount").abs())
        .when(pl.col("debit_credit_flag") == "D")
        .then(pl.col("amount").abs())
        .otherwise(None)
        .alias("amount")
    ])

    # Normalize account codes
    if has_columns(lf, "account_number"):
        lf = lf.with_columns([
            pl.col("account_number")
            .str.strip_chars()
            .str.to_uppercase()
            .alias("account_number")
        ])
    if has_columns(lf, "offset_account_number"):
        lf = lf.with_columns([
            pl.col("offset_account_number")
            .str.strip_chars()
            .str.to_uppercase()
            .alias("offset_account_number")
        ])

    #TODO: Add weekends, year, month, etc.

    current_user_id = 1 #TODO: FROM current_user
    #TODO: activate again
    view = {}
    # view = create_view(db, dataset_id, current_user_id, {}, {}, {})
    # file_path = os.path.join(BASE_DIR, settings.storage_views_dir, f"{view.id}.parquet")
    #
    # lf.sink_parquet(
    #     file_path,
    #     compression="zstd"
    # )

    return view
