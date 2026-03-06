from typing import Optional, List

from pydantic import BaseModel

from jet_engine.domain.models import (Dimension, FilterGroup, Measure, Sorting, 
                                      Pagination)


class ViewRequest(BaseModel):
    dimensions: List[Dimension]
    measures: List[Measure] = []
    filters: Optional[FilterGroup] = None
    sorting: List[Sorting] = []
    pagination: Optional[Pagination] = None
    parent_view_id: Optional[str] = None
    
#TODO: Use as view_request = ViewRequest.model_validate(json_payload)