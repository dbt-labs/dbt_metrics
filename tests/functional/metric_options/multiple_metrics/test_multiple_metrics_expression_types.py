from struct import pack
import os
import pytest
from dbt.tests.util import run_dbt

# our file contents
from tests.functional.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml,
)

# models/multiple_metrics.sql
multiple_metrics_sql = """
select *
from 
{{ dbt_metrics.calculate(
    [metric('base_sum_metric'), metric('expression_metric')],
    grain='month'
    )
}}
"""

# models/multiple_metrics.yml
multiple_metrics_yml = """
version: 2 
models:
  - name: multiple_metrics
    tests: 
      - dbt_utils.equality:
          compare_model: ref('multiple_metrics__expected')
metrics:
  - name: expression_metric
    label: Expression ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: expression
    sql: "{{metric('base_sum_metric')}} + 1"
    dimensions:
      - had_discount
      - order_country

  - name: base_sum_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: sum
    sql: order_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/multiple_metrics__expected.csv
multiple_metrics__expected_csv = """
date_month,base_sum_metric,expression_metric
2022-01-01,8,9
2022-02-01,6,7
""".lstrip()

class TestMultipleMetricsWithExpression:

    # configuration in dbt_project.yml
    # setting bigquery as table to get around query complexity 
    # resource constraints with compunding views
    if os.getenv('dbt_target') == 'bigquery':
        @pytest.fixture(scope="class")
        def project_config_update(self):
            return {
            "name": "example",
            "models": {"+materialized": "table"}
            }
    else: 
        @pytest.fixture(scope="class")
        def project_config_update(self):
            return {
            "name": "example",
            "models": {"+materialized": "view"}
            }  

    # install current repo as package
    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"local": os.getcwd()}
                ]
        }


    # everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "fact_orders_source.csv": fact_orders_source_csv,
            "multiple_metrics__expected.csv": multiple_metrics__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "multiple_metrics.sql": multiple_metrics_sql,
            "multiple_metrics.yml": multiple_metrics_yml
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 2

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 3

        # test tests
        results = run_dbt(["test"]) # expect passing test
        assert len(results) == 1

        # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]