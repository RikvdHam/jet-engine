import json
import hashlib
import uuid
from uuid import UUID
from dataclasses import dataclass
from typing import List, Dict, Optional, Any

from jet_engine.domain.models import (Dimension, Measure, Sorting, Pagination, 
                                      FilterGroup, MeasureSpec)
from jet_engine.domain.request_models import ViewRequest


@dataclass(frozen=True)
class View:
    id: Optional[UUID]
    dataset_id: UUID
    dimensions: List[Dimension]
    measures: List[MeasureSpec]
    filters: Optional[FilterGroup]
    sorting: List[Sorting]
    pagination: Optional[Pagination]
    parent_view_id: Optional[UUID]
    created_by: Optional[int]
    signature: Optional[str] = None

    @classmethod
    def from_request(cls, request: ViewRequest, dataset_id: str, created_by: int):
        expanded_measures = []
        for m in request.measures:
            expanded_measures.extend(m.expand())
            
        return cls(
            id=uuid.uuid4(),
            dataset_id=UUID(dataset_id),
            dimensions=request.dimensions,
            measures=expanded_measures,
            filters=request.filters,
            sorting=request.sorting,
            pagination=request.pagination,
            parent_view_id=UUID(request.parent_view_id) if request.parent_view_id else None,
            created_by=created_by
        )

    # ============================================================
    # Value Canonicalization
    # ============================================================
    def _canonicalize_value(self, value):

        if isinstance(value, list):
            return sorted(value)
        
        return value
    
    # ============================================================
    # Filter Canonicalization
    # ============================================================
    
    def _canonicalize_filter_node(self, node):
    
        # Filter condition
        if "field_id" in node:
            return {
                "type": "condition",
                "field_id": str(node["field_id"]),
                "operator": node["operator"],
                "value": self._canonicalize_value(node.get("value")),
            }
        
        # Filter group
        if "op" in node:
        
            children = [
                self._canonicalize_filter_node(c)
                for c in node.get("conditions", [])
            ]
            
            children_sorted = sorted(
                children,
                key=lambda x: json.dumps(x, sort_keys=True),
            )
            
            return {
                "type": "group",
                "op": node["op"],
                "conditions": children_sorted,
            }
        
        raise ValueError(f"Invalid filter node: {node}")
    
    def _canonicalize_filters(self):
    
        if self.filters is None:
            return None
        
        return self._canonicalize_filter_node(self.filters)
    
    # ============================================================
    # Dimension Canonicalization
    # ============================================================
    
    def _canonicalize_dimensions(self):
    
        return sorted(
            self.dimensions,
            key=lambda d: (
                str(d["field_id"]),
                d.get("axis"),
            ),
        )
    
    # ============================================================
    # Measure Canonicalization
    # ============================================================
    
    def _canonicalize_measures(self):
        normalized = []
        for m in self.measures:
            field_id = str(m["field_id"])
            for agg in sorted(m.get("aggregations", [])):
                normalized.append(
                    {
                        "field_id": field_id,
                        "aggregation": agg,
                    }
                )
        
        return sorted(
            normalized,
            key=lambda m: (
                m["field_id"],
                m["aggregation"],
            ),
        )
    
    # ============================================================
    # Signature Builder
    # ============================================================
    
    def build_signature(self) -> str:
        """
        Build deterministic SHA256 signature for the view.
        """
        
        canonical_payload = {
            "dataset_id": str(self.dataset_id),
            "filters": self._canonicalize_filters(),
            "dimensions": self._canonicalize_dimensions(),
            "measures": self._canonicalize_measures(),
        }
        
        canonical_json = json.dumps(
            canonical_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        
        return hashlib.sha256(
            canonical_json.encode("utf-8")
            ).hexdigest()