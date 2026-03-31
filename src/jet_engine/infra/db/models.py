from typing import List

from sqlalchemy import (Column, String, DateTime, Integer, Text, JSON, func, 
                        UniqueConstraint, ForeignKey, Index, desc)
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship, Session

from jet_engine.infra.db.base import Base
from jet_engine.domain.models import (View, FilterGroup, Dimension, 
                                      MeasureSpec, Sorting, Pagination)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    email = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, 
                        server_default=func.now())
    last_login_at = Column(DateTime)

    datasets = relationship("DatasetORM", back_populates="uploaded_by",
                            cascade="all, delete-orphan")


class DatasetORM(Base):
    __tablename__ = "datasets"

    id = Column(String, primary_key=True)
    company_name = Column(String, nullable=False)
    fiscal_year = Column(Integer, nullable=False)
    stored_filename = Column(String, nullable=False, unique=True)
    uploaded_by_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    uploaded_by = relationship("User", back_populates="datasets")
    uploaded_at = Column(DateTime(timezone=True), nullable=False,
                         server_default=func.now())
    original_filename = Column(String, nullable=False)
    signature = Column(Text, nullable=False)
    row_count = Column(Integer, nullable=False)
    data_hash = Column(String, nullable=False)
    last_accessed_at = Column(DateTime(timezone=True), nullable=False, 
                              server_default=func.now())
    views = relationship("ViewORM", back_populates="dataset")

    __table_args__ = (
        UniqueConstraint("stored_filename", name="uk_stored_filename"),
    )

    @staticmethod
    def load(db: Session, dataset_id: str):
        return db.query(DatasetORM).filter(DatasetORM.id == dataset_id).first()

    @staticmethod
    def load_latest_for_user(db: Session, user_id: int):
        return (
            db.query(DatasetORM)
            .filter(DatasetORM.uploaded_by_id == user_id)
            .order_by(desc(DatasetORM.last_accessed_at))
            .first()
        )


# class Field(Base):
#     __tablename__ = "fields"

#     id = Column(Integer, primary_key=True)
#     canonical_name = Column(String, nullable=False)
#     display_name = Column(String, nullable=False)
#     description = Column(Text, nullable=False)
#     data_type = Column(String, nullable=False)
#     is_required = Column(Boolean, nullable=False, server_default="False")
#     field_group = Column(String, nullable=False)

#     __table_args__ = (
#         UniqueConstraint("canonical_name", name="uk_name"),
#     )


# class FieldResponse(BaseModel):
#     id: int
#     canonical_name: str
#     display_name: str
#     description: str
#     data_type: str
#     is_required: bool
#     field_group: str

#     class Config:
#         from_attributes = True   # <-- THIS replaces orm_mode


class DatasetMapping(Base):
    __tablename__ = "dataset_mappings"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    mapping_json = Column(MutableDict.as_mutable(JSON), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    @staticmethod
    def load(db: Session, dataset_id: str):
        return (db.query(DatasetMapping)
                .filter(DatasetMapping.dataset_id == dataset_id)
                .first())


class SignatureMapping(Base):
    __tablename__ = "signature_mappings"

    id = Column(Integer, primary_key=True)
    signature = Column(String, nullable=False, unique=True)
    mapping_json = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    @staticmethod
    def load_mapping(db: Session, signature: str):
        return (db.query(SignatureMapping)
                .filter(SignatureMapping.signature == signature)
                .first())


class ViewORM(Base):
    __tablename__ = "dataset_views"

    id = Column(String, primary_key=True)
    dataset_id = Column(String, ForeignKey("datasets.id", ondelete="CASCADE"), 
                        nullable=False)

    # Full effective filters (ALL filters applied)
    filters_json = Column(JSON, nullable=True)

    # Grouping dimensions
    dimensions_json = Column(JSON, nullable=False)

    # Aggregations / measures
    measures_json = Column(JSON, nullable=False)
    
    sorting_json = Column(JSON, nullable=False)
    
    pagination = Column(JSON, nullable=True)

    # Deterministic hash of canonicalized definition
    signature = Column(String(64), nullable=False)

    # Optional lineage (NOT used for execution logic)
    parent_view_id = Column(Integer, 
                            ForeignKey("dataset_views.id", ondelete="SET NULL"), 
                            nullable=True)

    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, 
                        server_default=func.now())
    last_accessed_at = Column(DateTime(timezone=True), nullable=False, 
                              server_default=func.now())

    dataset = relationship("DatasetORM", back_populates="views")

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
    
    @classmethod
    def from_domain(cls, view: View):
        return cls(
            id=view.id,
            dataset_id=view.dataset_id,
            filters_json=view.filters.model_dump() if view.filters else None,
            dimensions_json=[d.model_dump() for d in view.dimensions],
            measures_json=[m.model_dump() for m in view.measures],
            sorting_json = [s.model_dump() for s in view.sorting] if view.sorting else None,
            pagination=view.pagination.model_dump() if view.pagination else None,
            signature=view.build_signature(),
            parent_view_id=view.parent_view_id,
            created_by=view.created_by 
        )
        
    def to_domain(self) -> View:
        return View(
            id=self.id,
            dataset_id=self.dataset_id,
            filters=FilterGroup.model_validate(self.filters_json),
            dimensions=[
                Dimension.model_validate(d) 
                for d in self.dimensions_json
            ],
            measures=[
                MeasureSpec.model_validate(s)
                for s in self.measures_json
            ],
            sorting=[
                Sorting.model_validate(s)
                for s in self.sorting_json
            ],
            pagination=Pagination.model_validate(self.pagination) \
                if self.pagination else None,
            signature=self.signature,
            parent_view_id=self.parent_view_id,
            created_by=self.created_by
        )

    @staticmethod
    def load(db: Session, signature: str):
        return db.query(ViewORM).filter_by(signature=signature).first()
