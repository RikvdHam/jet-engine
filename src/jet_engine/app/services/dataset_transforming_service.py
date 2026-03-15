import os
import uuid
from pathlib import Path

import polars as pl
from sqlalchemy.orm import Session
from fastapi import HTTPException

from jet_engine.infra.db.models import ViewORM, User, Dataset
from jet_engine.infra.core.config import settings
from jet_engine.app.services.dataset_query_service import create_initial_view


BASE_DIR = Path(__file__).resolve().parent.parent


def has_columns(lf: pl.LazyFrame, *cols: str) -> bool:
    existing = lf.collect_schema().names()
    return all(col in existing for col in cols)


def standardize_amounts(lf: pl.LazyFrame) -> pl.LazyFrame:
    if has_columns(lf, "debit_amount", "credit`s_amount"):
        # Compute signed amount and standardized flag
        lf = lf.with_columns([
            # amount = debit - abs(credit)
            (pl.coalesce([pl.col("debit_amount"), pl.lit(0)]) -
             pl.coalesce([pl.col("credit_amount").abs(), pl.lit(0)])
             ).alias("amount"),

            # debit_credit_indicator based on amount
            pl.when(pl.col("debit_amount").is_not_null() | pl.col("credit_amount").is_not_null())
            .then(
                pl.when((pl.col("debit_amount") - pl.col("credit_amount").abs()) > 0).then(pl.lit("D"))
                .when((pl.col("debit_amount") - pl.col("credit_amount").abs()) < 0).then(pl.lit("C"))
                .otherwise(pl.lit("N"))
            )
            .otherwise("UNKNOWN")
            .alias("debit_credit_indicator")
        ]).drop(["debit_amount", "credit_amount"])

    elif has_columns(lf, "amount", "debit_credit_indicator"):
        # Standardize flag to D/C/N/UNKNOWN
        lf = lf.with_columns([
            pl.col("debit_credit_indicator")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_lowercase()
            .str.slice(0, 1)
            .alias("_dc_first")
        ])
        lf = lf.with_columns([
            pl.when(pl.col("_dc_first") == "c")
            .then(pl.lit("C"))
            .when(pl.col("_dc_first") == "d")
            .then(pl.lit("D"))
            .otherwise(pl.lit("UNKNOWN"))
            .alias("debit_credit_indicator")
        ]).drop("_dc_first")

    else:
        raise HTTPException(status_code=400, detail="Unknown debit/credit column combination.")

    # Ensure amount matches sign convention
    lf = lf.with_columns([
        pl.when(pl.col("debit_credit_indicator") == "D").then(pl.col("amount").abs())
        .when(pl.col("debit_credit_indicator") == "C").then(-pl.col("amount").abs())
        .otherwise(pl.col("amount"))
        .alias("amount")
    ])

    return lf


def standardize_account_codes(lf: pl.LazyFrame) -> pl.LazyFrame:
    if has_columns(lf, "account_number"):
        lf = lf.with_columns([
            pl.col("account_number")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("account_number")
        ])

    if has_columns(lf, "offset_account_number"):
        lf = lf.with_columns([
            pl.col("offset_account_number")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("offset_account_number")
        ])

    return lf


def enrich_with_helper_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    exprs = []

    # -------------------------
    # Monetary helpers
    # -------------------------
    if has_columns(lf, "amount"):
        exprs += [
            pl.col("amount").abs().alias("abs_amount"),
            pl.when(pl.col("amount") > 0)
            .then(1)
            .otherwise(-1)
            .alias("amount_sign")
        ]

        abs_amount = pl.col("amount").abs().cast(pl.Int64).cast(pl.Utf8)

        exprs += [
            abs_amount.str.slice(0, 1).alias("first_digit"),
            abs_amount.str.slice(0, 2).alias("first_two_digits"),
        ]

    # -------------------------
    # Date helpers
    # -------------------------
    if has_columns(lf, "posting_date"):
        posting_date = pl.col("posting_date").str.strptime(pl.Date, strict=False)

        exprs += [
            posting_date.dt.year().alias("posting_year"),
            posting_date.dt.month().alias("posting_month"),
            posting_date.dt.day().alias("posting_day"),
            posting_date.dt.weekday().alias("posting_weekday"),
        ]

    # -------------------------
    # Time helpers
    # -------------------------
    if has_columns(lf, "posting_time"):
        posting_time = pl.col("posting_time").str.strptime(pl.Time, strict=False)

        exprs += [
            posting_time.dt.hour().alias("posting_hour"),
            posting_time.dt.minute().alias("posting_minute"),
        ]

    # -------------------------
    # Text helpers
    # -------------------------
    if has_columns(lf, "description"):
        exprs += [
            pl.col("description").str.len_chars().alias("description_length"),
            pl.col("description").is_not_null().alias("has_description"),
        ]

    if has_columns(lf, "reference"):
        exprs += [
            pl.col("reference").is_not_null().alias("has_reference"),
        ]

    if exprs:
        lf = lf.with_columns(exprs)

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

    # --- 6. Add helper columns
    lf = enrich_with_helper_columns(lf)

    view = create_initial_view(db, dataset_id, current_user_id)
    file_path = Path(settings.storage_transformed_dir) / f"{dataset_id}.parquet"

    # Ensure parent directory exists
    file_path.parent.mkdir(parents=True, exist_ok=True)

    lf.sink_parquet(
        file_path,
        compression="zstd"
    )

    return view
