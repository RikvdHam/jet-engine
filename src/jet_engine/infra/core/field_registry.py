import os
import uuid
import json
from pathlib import Path
from typing import List, Optional

from jet_engine.infra.core.config import settings
from jet_engine.domain.models.field import Field


BASE_DIR = Path(__file__).resolve().parent.parent.parent


class FieldRegistry:
    
    def __init__(self):
        json_path = os.path.join(BASE_DIR, settings.storage_static_dir, 
                                 "fields.json")
        with open(json_path) as f:
            data = json.load(f)
        
        self._fields = [Field.model_validate(f) for f in data]
        self._registry = {str(f.id): f for f in self._fields}

    def get_field(self, field_id: str) -> Field:
        field: Field = self._registry.get(field_id, None)
        if not field:
            raise Exception(f"Unknown field ID: {field_id}")

        return field

    def all(self, field_ids: Optional[List] = None) -> List[Field]:
        if not field_ids:
            return self._fields

        return [self.get_field(fid) for fid in field_ids]


field_registry = FieldRegistry()
