from typing import Dict

from pydantic import BaseModel


class MappingRequest(BaseModel):
    mapping: Dict[str, str]
