import json
import hashlib
from pydantic import BaseModel
from typing import List

from sqlalchemy import Column, String, Boolean, DateTime, Integer, Text, JSON, func, UniqueConstraint, ForeignKey, Index
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship, Session

from jet_engine.db.base import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    email = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_login_at = Column(DateTime)

    datasets = relationship("Dataset", back_populates="uploaded_by", cascade="all, delete-orphan")


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(String, primary_key=True)
    company_name = Column(String, nullable=False)
    fiscal_year = Column(Integer, nullable=False)
    stored_filename = Column(String, nullable=False, unique=True)
    uploaded_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    uploaded_by = relationship("User", back_populates="datasets")
    upload_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    original_filename = Column(String, nullable=False)
    signature = Column(Text, nullable=False)
    row_count = Column(Integer, nullable=False)
    data_hash = Column(String, nullable=False)
    last_accessed_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    views = relationship("DatasetView", back_populates="dataset")

    __table_args__ = (
        UniqueConstraint("stored_filename", name="uk_stored_filename"),
    )

    @staticmethod
    def load(db: Session, dataset_id: str):
        return db.query(Dataset).filter(Dataset.id == dataset_id).first()


class Field(Base):
    __tablename__ = "fields"

    id = Column(Integer, primary_key=True)
    canonical_name = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    data_type = Column(String, nullable=False)
    is_required = Column(Boolean, nullable=False, server_default="False")
    field_group = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("canonical_name", name="uk_name"),
    )


class FieldResponse(BaseModel):
    id: int
    canonical_name: str
    display_name: str
    description: str
    data_type: str
    is_required: bool
    field_group: str

    class Config:
        from_attributes = True   # <-- THIS replaces orm_mode


class DatasetMapping(Base):
    __tablename__ = "dataset_mappings"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    mapping_json = Column(MutableDict.as_mutable(JSON), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    @staticmethod
    def load(db: Session, dataset_id: str):
        return db.query(DatasetMapping).filter(DatasetMapping.dataset_id == dataset_id).first()


class SignatureMapping(Base):
    __tablename__ = "signature_mappings"

    id = Column(Integer, primary_key=True)
    signature = Column(String, nullable=False, unique=True)
    mapping_json = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    @staticmethod
    def load_mapping(db: Session, signature: str):
        return db.query(SignatureMapping).filter(SignatureMapping.signature == signature).first()


class DatasetView(Base):
    __tablename__ = "dataset_views"

    id = Column(String, primary_key=True)
    dataset_id = Column(String, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)

    # Full effective filters (ALL filters applied)
    filters_json = Column(JSON, nullable=False)

    # Grouping dimensions
    dimensions_json = Column(JSON, nullable=False)

    # Aggregations / measures
    measures_json = Column(JSON, nullable=False)

    # Deterministic hash of canonicalized definition
    signature = Column(String(64), nullable=False)

    # Optional lineage (NOT used for execution logic)
    parent_view_id = Column(Integer, ForeignKey("dataset_views.id", ondelete="SET NULL"), nullable=True)

    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_accessed_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    dataset = relationship("Dataset", back_populates="views")

    __table_args__ = (
        # Prevent duplicate logical views
        UniqueConstraint(
            "dataset_id",
            "signature",
            name="ux_dataset_signature",
        ),

        # Fast lookup per dataset
        Index(
            "ix_dataset_views_dataset_id",
            "dataset_id",
        ),

        # Lineage lookups
        Index(
            "ix_dataset_views_parent_view_id",
            "parent_view_id",
        ),

        # Cache eviction / LRU queries
        Index(
            "ix_dataset_views_last_accessed_at",
            "last_accessed_at",
        ),
    )

    @staticmethod
    def load(db: Session, signature: str):
        return db.query(DatasetView).filter_by(signature=signature).first()

    # ==========================
    # Signature Logic
    # ==========================

    @staticmethod
    def _canonicalize_filters(filters):
        """
        Sort filters deterministically.
        Assumes simple AND filters.
        """
        return sorted(
            filters,
            key=lambda f: (
                f.get("field"),
                f.get("operator"),
                json.dumps(f.get("value"), sort_keys=True),
            ),
        )

    @staticmethod
    def _canonicalize_dimensions(dimensions):
        return sorted(dimensions)

    @staticmethod
    def _canonicalize_measures(measures):
        return sorted(
            measures,
            key=lambda m: (
                m.get("field"),
                m.get("aggregation"),
                m.get("alias"),
            ),
        )

    @classmethod
    def build_signature(
            cls,
            dataset_id,
            filters,
            dimensions,
            measures,
    ):
        """
        Build deterministic SHA256 signature from canonicalized definition.
        """

        canonical_payload = {
            "dataset_id": str(dataset_id),
            "filters": cls._canonicalize_filters(filters or []),
            "dimensions": cls._canonicalize_dimensions(dimensions or []),
            "measures": cls._canonicalize_measures(measures or []),
        }

        canonical_json = json.dumps(
            canonical_payload,
            sort_keys=True,
            separators=(",", ":"),  # removes whitespace
        )

        return hashlib.sha256(
            canonical_json.encode("utf-8")
        ).hexdigest()
