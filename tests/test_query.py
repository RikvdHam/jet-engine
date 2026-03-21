import uuid

from fastapi import Response

from jet_engine.infra.core.query_builder import QueryBuilder
from jet_engine.domain.enums import FieldRole, Aggregation, FilterOperator, Axis, LogicalOperator
from jet_engine.domain.models import View, Dimension, Measure, FilterCondition, FilterGroup, MeasureSpec
from jet_engine.infra.core import field_registry


def test_query_endpoint_success(client, mocker):

    mocker.patch(
        "jet_engine.app.api.routes.datasets.execute_query",
        return_value=Response(
            content=b"arrowdata",
            media_type="application/vnd.apache.arrow.stream"
        )
    )

    payload = {
        "dimensions": [],
        "measures": [],
        "filters": None
    }

    response = client.post("/api/datasets/123e4567-e89b-12d3-a456-426614174000/query", json=payload)

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"


def test_querybuilder_build_complex_view(mocker):
    """
    Test QueryBuilder.build() with:
    - multiple dimensions
    - multiple measures (different aggregations)
    - GROUP BY multiple dimensions
    - nested filters (AND + OR)
    - pagination
    """

    # --- 1. Generate UUIDs for each field
    country_id = uuid.uuid4()
    year_id = uuid.uuid4()
    amount_id = uuid.uuid4()
    transactions_id = uuid.uuid4()

    # --- 2. MockField factory
    def MockFieldFactory(field_id):
        mapping = {
            country_id: "country",
            year_id: "year",
            amount_id: "amount",
            transactions_id: "transactions"
        }
        canonical_name = mapping[field_id]

        class MockField:
            def __init__(self):
                self.id = field_id
                self.canonical_name = canonical_name
                self.display_name = canonical_name.capitalize()
                self.description = f"Mock description for {canonical_name}"
                self.dtype = "string"

                self.roles = [FieldRole.DIMENSION, FieldRole.MEASURE, FieldRole.FILTER]
                self.aggregations = [Aggregation.SUM, Aggregation.AVG]
                self.filter_operators = [FilterOperator.EQ, FilterOperator.GT, FilterOperator.LT, FilterOperator.IN]

                self.is_required = False
                self.is_mandatory = True
                self.group = None

            def allows_role(self, role): return role in self.roles
            def allows_aggregation(self, agg): return agg in self.aggregations
            def allows_filter_operator(self, op): return op in self.filter_operators

        return MockField()

    # --- 3. Patch the field_registry safely
    mocker.patch(
        "jet_engine.infra.core.query_builder.field_registry.get_field",
        side_effect=MockFieldFactory
    )

    # --- 4. Define Dimensions & Measures
    dimensions = [
        Dimension(field_id=country_id, axis=Axis.ROW),
        Dimension(field_id=year_id, axis=Axis.COLUMN)
    ]

    measures = [
        Measure(field_id=amount_id, aggregations=[Aggregation.SUM]),
        Measure(field_id=transactions_id, aggregations=[Aggregation.AVG])
    ]

    # --- 5. Define nested filters: (country='US' AND year>2020) OR (amount>1000)
    filter_tree = FilterGroup(
        op=LogicalOperator.OR,
        conditions=[
            FilterGroup(
                op=LogicalOperator.AND,
                conditions=[
                    FilterCondition(field_id=country_id, operator=FilterOperator.EQ, value="US"),
                    FilterCondition(field_id=year_id, operator=FilterOperator.GT, value=2020)
                ]
            ),
            FilterCondition(field_id=amount_id, operator=FilterOperator.GT, value=1000)
        ]
    )

    # --- 6. Pagination
    pagination = type("Pagination", (), {"limit": 50, "offset": 10})()

    # --- 7. Create the View
    view = View(
        id=None,
        dataset_id="dataset1",
        dimensions=dimensions,
        measures=measures,
        filters=filter_tree,
        pagination=pagination,
        sorting=[],
        created_by=1,
        parent_view_id=None
    )

    # --- 8. Build the query
    query = QueryBuilder.build(view)

    sql = query.sql
    params = query.params

    # --- 9. Assertions

    # SELECT clause
    assert "country" in sql
    assert "year" in sql
    assert "SUM(amount)" in sql
    assert "AVG(transactions)" in sql

    # GROUP BY clause
    assert "GROUP BY country, year" in sql

    # WHERE clause contains AND/OR
    assert "(" in sql and ")" in sql
    assert "AND" in sql
    assert "OR" in sql

    # Pagination
    assert "LIMIT ?" in sql
    assert "OFFSET ?" in sql
    assert params[-2:] == [50, 10]

    # Filter values
    assert "US" in params
    assert 2020 in params
    assert 1000 in params