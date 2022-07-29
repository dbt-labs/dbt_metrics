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

# models/metric_on_expression_metric.sql
metric_on_expression_metric_sql = """
select *
from 
{{ metrics.calculate(metric('metric_on_expression_metric'), 
    grain='month'
    )
}}
"""

# models/base_sum_metric.yml
base_sum_metric_yml = """
version: 2 
metrics:
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

# models/expression_metric.yml
metric_on_expression_metric_yml = """
version: 2 
models:
  - name: metric_on_expression_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('metric_on_expression_metric__expected')
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

  - name: metric_on_expression_metric
    label: Expression ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: expression
    sql: "{{metric('expression_metric')}} + 1"
    dimensions:
      - had_discount
      - order_country
"""

# seeds/metric_on_expression_metric__expected.csv
metric_on_expression_metric__expected_csv = """
date_month,base_sum_metric,expression_metric,metric_on_expression_metric
2022-02-01,6,7,8
2022-01-01,8,9,10
""".lstrip()

class TestMetricOnExpressionMetric:

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
          "name": "example",
          "models": {"+materialized": "table"}
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
            "metric_on_expression_metric__expected.csv": metric_on_expression_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "base_sum_metric.yml": base_sum_metric_yml,
            "metric_on_expression_metric.yml": metric_on_expression_metric_yml,
            "fact_orders.sql": fact_orders_sql,
            "metric_on_expression_metric.sql": metric_on_expression_metric_sql
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

        # # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]