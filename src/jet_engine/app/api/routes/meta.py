from fastapi import APIRouter, Depends, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

# from jet_engine.infra.db.models import FieldResponse
# from jet_engine.infra.db.session import get_db
from jet_engine.domain.models import Field
from jet_engine.infra.core import field_registry


router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.get("/fields") #, response_model=list[FieldResponse]
@limiter.limit("10/minute")
async def get_fields(request: Request):
    return field_registry.all()


# FIELDS = [
#
#     # ─────────────
#     # IDENTIFICATION
#     # ─────────────
#     {
#         "id": 1,
#         "canonical_name": "document_number",
#         "display_name": "Document Number",
#         "description": "Accounting document identifier",
#         "value_type": "string",
#         "is_required": False,
#         "group": "identification"
#     },
#     {
#         "id": 2,
#         "canonical_name": "transaction_number",
#         "display_name": "Transaction Number",
#         "description": "Internal transaction tracking identifier",
#         "value_type": "string",
#         "is_required": False,
#         "group": "identification"
#     },
#     {
#         "id": 3,
#         "canonical_name": "line_number",
#         "display_name": "Line Number",
#         "description": "Line number within a journal entry",
#         "value_type": "integer",
#         "is_required": False,
#         "group": "identification"
#     },
#
#     # ─────────────
#     # DATES & TIME
#     # ─────────────
#     {
#         "id": 4,
#         "canonical_name": "posting_date",
#         "display_name": "Posting Date",
#         "description": "Date the journal entry was posted to the ledger",
#         "value_type": "date",
#         "is_required": True,
#         "group": "date"
#     },
#     {
#         "id": 5,
#         "canonical_name": "entry_date",
#         "display_name": "Entry Date",
#         "description": "Date the journal entry was created",
#         "value_type": "date",
#         "is_required": False,
#         "group": "date"
#     },
#     {
#         "id": 6,
#         "canonical_name": "posting_time",
#         "display_name": "Posting Time",
#         "description": "Time the journal entry was posted",
#         "value_type": "datetime",
#         "is_required": False,
#         "group": "date"
#     },
#
#     # ─────────────
#     # MONETARY (Flexible Patterns)
#     # ─────────────
#     {
#         "id": 7,
#         "canonical_name": "amount",
#         "display_name": "Amount",
#         "description": "Single amount column (may require Debit/Credit indicator)",
#         "value_type": "float",
#         "is_required": False,
#         "group": "monetary"
#     },
#     {
#         "id": 8,
#         "canonical_name": "debit_credit_indicator",
#         "display_name": "Debit / Credit Indicator",
#         "description": "Indicator whether entry is Debit or Credit",
#         "value_type": "string",
#         "is_required": False,
#         "group": "monetary"
#     },
#     {
#         "id": 9,
#         "canonical_name": "debit_amount",
#         "display_name": "Debit Amount",
#         "description": "Debit value when separate debit and credit columns are provided",
#         "value_type": "float",
#         "is_required": False,
#         "group": "monetary"
#     },
#     {
#         "id": 10,
#         "canonical_name": "credit_amount",
#         "display_name": "Credit Amount",
#         "description": "Credit value when separate debit and credit columns are provided",
#         "value_type": "float",
#         "is_required": False,
#         "group": "monetary"
#     },
#     {
#         "id": 11,
#         "canonical_name": "currency",
#         "display_name": "Currency",
#         "description": "Currency of the journal entry",
#         "value_type": "string",
#         "is_required": False,
#         "group": "monetary"
#     },
#
#     # ─────────────
#     # ACCOUNTS
#     # ─────────────
#     {
#         "id": 12,
#         "canonical_name": "account_number",
#         "display_name": "Account Number",
#         "description": "Primary ledger account number",
#         "value_type": "string",
#         "is_required": True,
#         "group": "account"
#     },
#     {
#         "id": 13,
#         "canonical_name": "account_name",
#         "display_name": "Account Name",
#         "description": "Name of the primary ledger account",
#         "value_type": "string",
#         "is_required": False,
#         "group": "account"
#     },
#     {
#         "id": 14,
#         "canonical_name": "offset_account_number",
#         "display_name": "Offset Account Number",
#         "description": "Balancing or counterparty account number",
#         "value_type": "string",
#         "is_required": False,
#         "group": "account"
#     },
#
#     # ─────────────
#     # USER & WORKFLOW
#     # ─────────────
#     {
#         "id": 15,
#         "canonical_name": "user_id",
#         "display_name": "User ID",
#         "description": "Identifier of the user who created the entry",
#         "value_type": "string",
#         "is_required": False,
#         "group": "workflow"
#     },
#     {
#         "id": 16,
#         "canonical_name": "approver_id",
#         "display_name": "Approver ID",
#         "description": "Identifier of the approving user",
#         "value_type": "string",
#         "is_required": False,
#         "group": "workflow"
#     },
#     {
#         "id": 17,
#         "canonical_name": "approval_status",
#         "display_name": "Approval Status",
#         "description": "Workflow approval status of the journal entry",
#         "value_type": "string",
#         "is_required": False,
#         "group": "workflow"
#     },
#
#     # ─────────────
#     # CLASSIFICATION
#     # ─────────────
#     {
#         "id": 18,
#         "canonical_name": "journal_type",
#         "display_name": "Journal Type",
#         "description": "Source classification (Manual or System)",
#         "value_type": "string",
#         "is_required": False,
#         "group": "classification"
#     },
#     {
#         "id": 19,
#         "canonical_name": "journal_category",
#         "display_name": "Journal Category",
#         "description": "Category or batch type of the journal entry",
#         "value_type": "string",
#         "is_required": False,
#         "group": "classification"
#     },
#
#     # ─────────────
#     # DESCRIPTION / METADATA
#     # ─────────────
#     {
#         "id": 20,
#         "canonical_name": "description",
#         "display_name": "Description",
#         "description": "Narrative description of the journal entry",
#         "value_type": "string",
#         "is_required": False,
#         "group": "metadata"
#     },
#     {
#         "id": 21,
#         "canonical_name": "reference",
#         "display_name": "Reference",
#         "description": "External reference or supporting document reference",
#         "value_type": "string",
#         "is_required": False,
#         "group": "metadata"
#     }
# ]
#
# @router.get("/fields/populate")
# async def populate_fields(db: Session = Depends(get_db)):
#     for f in FIELDS:
#         field = Field(
#             canonical_name=f.get('canonical_name'),
#             display_name=f.get('display_name'),
#             description=f.get('description'),
#             data_type=f.get('value_type'),
#             is_required=f.get('is_required'),
#             field_group=f.get('group')
#         )
#         db.add(field)
#     db.commit()
#
#     return {}
