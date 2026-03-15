import os
import uuid
from pathlib import Path

import polars as pl
from sqlalchemy.orm import Session
from fastapi import HTTPException

from jet_engine.infra.db.models import ViewORM, User, Dataset
from jet_engine.infra.core.config import settings
# from jet_engine.app.services.dataset_query_service import create_view


BASE_DIR = Path(__file__).resolve().parent.parent


def has_columns(lf: pl.LazyFrame, *cols: str) -> bool:
    existing = lf.collect_schema().names()
    return all(col in existing for col in cols)


def standardize_amounts(lf: pl.LazyFrame) -> pl.LazyFrame:
    if has_columns(lf, "debit_amount", "credit_amount"):
        # Compute signed amount and standardized flag
        lf = lf.with_columns([
            # amount = debit - abs(credit)
            (pl.coalesce([pl.col("debit_amount"), pl.lit(0)]) -
             pl.coalesce([pl.col("credit_amount").abs(), pl.lit(0)])
             ).alias("amount"),

            # debit_credit_flag based on amount
            pl.when(pl.col("debit_amount").is_not_null() | pl.col("credit_amount").is_not_null())
            .then(
                pl.when((pl.col("debit_amount") - pl.col("credit_amount").abs()) > 0).then("D")
                .when((pl.col("debit_amount") - pl.col("credit_amount").abs()) < 0).then("C")
                .otherwise("N")
            )
            .otherwise("UNKNOWN")
            .alias("debit_credit_flag")
        ]).drop(["debit_amount", "credit_amount"])

    elif has_columns(lf, "amount", "debit_credit_flag"):
        # Standardize flag to D/C/N/UNKNOWN
        lf = lf.with_columns([
            pl.col("debit_credit_flag")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_lowercase()
            .str.slice(0, 1)
            .alias("_dc_first")
        ])
        lf = lf.with_columns([
            pl.when(pl.col("_dc_first") == "d").then("D")
            .when(pl.col("_dc_first") == "c").then("C")
            .when(pl.col("_dc_first") == "n").then("N")
            .otherwise("UNKNOWN")
            .alias("debit_credit_flag")
        ]).drop("_dc_first")

    else:
        raise HTTPException(status_code=400, detail="Unknown debit/credit column combination.")

    # Ensure amount matches sign convention
    lf = lf.with_columns([
        pl.when(pl.col("debit_credit_flag") == "D").then(pl.col("amount").abs())
        .when(pl.col("debit_credit_flag") == "C").then(-pl.col("amount").abs())
        .otherwise(pl.col("amount"))
        .alias("amount")
    ])

    return lf


def standardize_account_codes(lf: pl.LazyFrame) -> pl.LazyFrame:
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

    return lf


def transform_dataset(db: Session, dataset_id: str, current_user_id: int) -> ViewORM: #TODO: Is it?
    # --- 1. Check is dataset exists
    dataset = Dataset.load(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # --- 2. Check if validated data file exists
    data_path = os.path.join(BASE_DIR, settings.storage_validated_dir, dataset.stored_filename)
    if not os.path.exists(data_path):
        raise HTTPException(status_code=404, detail="Validated file missing")

    # --- 3. Lazy scan validated parquet file
    lf = (
        pl.scan_parquet(data_path)
        .filter(pl.col("is_valid") == True)
    )

    # --- 4. Standardize debit/credit amounts
    lf = standardize_amounts(lf)

    # --- 5. Standardize account codes
    lf = standardize_account_codes(lf)


    #TODO: Add weekends, year, month, absolute amount, etc.

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
