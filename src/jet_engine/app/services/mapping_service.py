from sqlalchemy.orm import Session
from fastapi import HTTPException

from jet_engine.infra.db.models import Dataset, DatasetMapping, SignatureMapping
from jet_engine.infra.core import field_registry


def validate_map(mapping: dict) -> None:
    valid_fields = {str(field.id) for field in field_registry.all()}

    invalid_fields = set(mapping.values()) - valid_fields
    if invalid_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid field ids: {list(invalid_fields)}"
        )

    if len(mapping.values()) != len(set(mapping.values())):
        raise HTTPException(
            status_code=400,
            detail="Duplicate canonical fields in mapping"
        )


async def save_map(dataset_id: str,
                       mapping: dict,  # raw_column -> field_id
                       db: Session) -> None:

    dataset = Dataset.load(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    validate_map(mapping)

    # Save mapping for this dataset
    existing_mapping = DatasetMapping.load(db, dataset_id)
    if existing_mapping:
        existing_mapping.mapping_json = mapping
    else:
        db.add(DatasetMapping(dataset_id=dataset_id, mapping_json=mapping))

    # Save mapping for future reference if new signature
    existing_signature = SignatureMapping.load_mapping(db, dataset.signature)
    if not existing_signature:
        db.add(SignatureMapping(signature=dataset.signature, mapping_json=mapping))

    db.commit()
