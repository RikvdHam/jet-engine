from typing import List, Dict, Tuple

from jet_engine.infra.core import field_registry
from jet_engine.domain.enums import FieldRole, FilterOperator
from jet_engine.domain.models import Dimension, Measure, View, FilterNode, FilterCondition, FilterGroup


class QueryBuilder:

    @staticmethod
    def build(view: View) -> Tuple[str, List]:
        dimensions = QueryBuilder._compile_dimensions(view.dimensions)
        measures = QueryBuilder._compile_measures(view.measures)
        filter_sql, params = QueryBuilder._compile_filters(view.filters)

        return (
                   f"{QueryBuilder._compile_select_clause(view.dataset_id, dimensions, measures)}"
                   f"{QueryBuilder._compile_where_clause(filter_sql)}"
                   f"{QueryBuilder._compile_group_by_class(dimensions)}"
               ), params

    @staticmethod
    def _compile_select_clause(dataset_id: str, dimensions: List[str],
                               measures: List[str]) -> str:
        select_parts = dimensions + measures
        if not select_parts:
            return (
                f"SELECT *\n"
                f"FROM read_parquet('{dataset_id}.parquet')\n"
            )

        return (
            f"SELECT {', '.join(select_parts)}\n"
            f"FROM read_parquet('{dataset_id}.parquet')\n"
        )

    @staticmethod
    def _compile_where_clause(filter_sql: str) -> str:
        if not filter_sql:
            return ""

        return f"WHERE {filter_sql}\n"

    @staticmethod
    def _compile_group_by_class(dimensions: List[str]) -> str:
        if not dimensions:
            return ""

        return "GROUP BY " + ", ".join(dimensions)

    @staticmethod
    def _compile_dimensions(dimensions: List[Dimension]) -> List:
        dimension_list = []
        if not dimensions:
            return dimension_list

        for dimension in dimensions:
            field_id = dimension.field_id
            if not field_id:
                raise Exception(f"No field ID for dimension: {dimension}")

            field = field_registry.get_field(field_id)
            if not field.allows_role(FieldRole.DIMENSION):
                raise Exception(f"Field has not a dimension role: {field}")

            dimension_list.append(field.canonical_name)

        return dimension_list

    @staticmethod
    def _compile_measures(measures: List[Measure]) -> List:
        measure_list = []
        if not measures:
            return measure_list

        for measure in measures:
            field_id = measure.field_id
            field = field_registry.get_field(field_id)

            if not field.allows_role(FieldRole.MEASURE):
                raise Exception(f"Field has not a measure role: {field}")

            aggregation = measure.aggregation
            if not field.allows_aggregation(aggregation):
                raise Exception(f"Aggregation {aggregation.name} not allowed "
                                f"for field: {field}")

            measure_list.append(f"{aggregation.name}({field.canonical_name}) AS "
                                f"{field.canonical_name}_{aggregation.value}")

        return measure_list

    @staticmethod
    def _compile_filters(node: FilterNode) -> Tuple[str, List]:
        """
        Compile filter tree to SQL fragment and parameter list.
        """

        if node is None:
            return "", []

        # GROUP NODE (AND / OR)
        if isinstance(node, FilterGroup):
            op = node.op.value.upper()  # assuming enum like "and"/"or"

            fragments = []
            params = []

            for child in node.conditions:
                frag, child_params = QueryBuilder._compile_filters(child)

                if frag:
                    fragments.append(frag)
                    params.extend(child_params)

            if not fragments:
                return "", []

            return "(" + f" {op} ".join(fragments) + ")", params

        # LEAF NODE
        if isinstance(node, FilterCondition):
            field_id = node.field_id
            field = field_registry.get_field(field_id)

            if not field.allows_role(FieldRole.FILTER):
                raise Exception(f"Field has not a filter role: {field}")

            filter_operator = node.operator

            if not field.allows_filter_operator(filter_operator):
                raise Exception(
                    f"Filter operator {filter_operator.name} not allowed for field: {field}"
                )

            value = node.value
            params = []

            # IN / NOT IN
            if filter_operator.name in ("IN", "NOT_IN"):
                placeholders = ", ".join(["?"] * len(value))
                fragment = f"{field.canonical_name} {filter_operator.value} ({placeholders})"
                params.extend(value)
                return fragment, params

            fragment = f"{field.canonical_name} {filter_operator.value} ?"
            params.append(value)

            return fragment, params

        raise Exception(f"Unknown filter node type: {type(node)}")
