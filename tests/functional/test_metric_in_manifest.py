import pytest
from dbt.tests.util import run_dbt, get_manifest
from dbt.exceptions import ParsingException

# our file contents
from tests.functional.fixtures import (
    seed_slack_users_csv,
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml
)

metrics__base_average_metric_yml = """
version: 2 
metrics:
  - name: base_average_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week]
    type: average
    sql: discount_total
    dimensions:
      - had_discount
      - order_country
"""

class TestSimpleMetrics:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_average_metric.yml": metrics__base_average_metric_yml,
            "fact_orders.sql": fact_orders_sql,
        }

    def test_metric_in_manifest(
        self,
        project,
    ):
        # initial run
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        metric_ids = list(manifest.metrics.keys())
        expected_metric_ids = ["metric.dbt_metrics_integration_tests.base_average_metric"]
        assert metric_ids == expected_metric_ids