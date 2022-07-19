import pytest
from dbt.tests.util import run_dbt, get_manifest
from dbt.exceptions import ParsingException

# our file contents
from integration_tests.tests.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml
)

# models/base_average_metric.sql
metrics__base_average_metric_sql = """
select *
from 
{{ metrics.calculate(metric('base_average_metric'), 
    grain='month', 
    dimensions=['had_discount']) 
}}
"""

# models/base_average_metric.yml
metrics__base_average_metric_yml = """
version: 2 
models:
  - name: base_average_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('base_average_metric__expected')

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

# seeds/base_average_metric__expected.csv
base_average_metric__expected_csv = """
date_month,had_discount,base_average_metric
2022-01-01,TRUE,1.000000
2022-01-01,FALSE,1.000000
2022-02-01,FALSE,1.000000
2022-02-01,TRUE,1.000000
""".lstrip()

class TestAverageMetric:

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
          "name": "example",
          "models": {"+materialized": "view"}
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_average_metric.yml": metrics__base_average_metric_yml,
            "fact_orders.sql": fact_orders_sql,
        }

    # everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "fact_orders_source.csv": fact_orders_source_csv,
            "base_average_metric__expected.csv": base_average_metric__expected_csv,
        }

    def test_metric_in_manifest(
        self,
        project,
    ):
        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 2

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 2

        # test tests
        results = run_dbt(["test"], expect_pass = True) # expect passing test
        assert len(results) == 1

        # validate that the results include one pass and one failure
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]