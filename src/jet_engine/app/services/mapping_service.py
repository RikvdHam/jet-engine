from sqlalchemy.orm import Session
from fastapi import HTTPException

from jet_engine.infra.db.models import DatasetORM, DatasetMapping, SignatureMapping
from jet_engine.infra.core import field_registry


async def validate_map(mapping: dict) -> None:
    """
    Validate a user-provided mapping of CSV column -> Field ID.

    Rules:
    1. Field IDs must exist in registry
    2. No duplicate Field IDs
    3. All mandatory fields must be mapped
    4. Either (debit_amount & credit_amount) OR (amount & debit_credit_indicator) must be mapped
    """

    # --- 1. Check if fieldID's exist in registry
    valid_fields = {str(field.id) for field in field_registry.all()}

    invalid_fields = set(mapping.values()) - valid_fields
    if invalid_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid field ids: {list(invalid_fields)}"
        )

    # --- 2. Check on duplicate fieldID's
    if len(mapping.values()) != len(set(mapping.values())):
        raise HTTPException(
            status_code=400,
            detail="Duplicate canonical fields in mapping"
        )

    # --- 3. Check if mandatory fields are mapped
    mandatory_fields = {str(f.id) for f in field_registry.all() if f.is_mandatory}
    mapped_fields = set(mapping.values())
    missing_mandatory = mandatory_fields - mapped_fields
    if missing_mandatory:
        missing_names = [field_registry.get_field(fid).canonical_name for fid in missing_mandatory]
        raise HTTPException(
            status_code=400,
            detail=f"Mandatory fields not mapped: {missing_names}"
        )

    # --- 4. Debit/Credit rule check
    # Get IDs for relevant fields
    debit_amount_id = next((str(f.id) for f in field_registry.all() if f.canonical_name == "debit_amount"), None)
    credit_amount_id = next((str(f.id) for f in field_registry.all() if f.canonical_name == "credit_amount"), None)
    amount_id = next((str(f.id) for f in field_registry.all() if f.canonical_name == "amount"), None)
    debit_credit_indicator_id = next(
        (str(f.id) for f in field_registry.all() if f.canonical_name == "debit_credit_indicator"), None)

    # Check which groups are mapped
    debit_credit_group_1 = debit_amount_id in mapped_fields and credit_amount_id in mapped_fields
    debit_credit_group_2 = amount_id in mapped_fields and debit_credit_indicator_id in mapped_fields

    if not (debit_credit_group_1 or debit_credit_group_2):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid debit/credit mapping. You must map either "
                "both 'Debit Amount' & 'Credit Amount' OR "
                "'Amount' & 'Debit / Credit Indicator'."
            )
        )

    # Ensure groups are not mixed
    if debit_credit_group_1 and debit_credit_group_2:
        raise HTTPException(
            status_code=400,
            detail="Cannot mix debit/credit field groups. Use only one group."
        )


async def save_map(dataset_id: str, mapping: dict, db: Session) -> None:

    dataset = DatasetORM.load(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

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
