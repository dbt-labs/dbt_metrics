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

# models/rolling_expression_metric.sql
rolling_expression_metric_sql = """
select *
from 
{{ metrics.calculate(metric('rolling_expression_metric'), 
    grain='month',
    secondary_calculations=[
        metrics.rolling(aggregate="max", interval=2),
        metrics.rolling(aggregate="min", interval=2),
        metrics.rolling(aggregate="sum", interval=2)
    ]
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

# models/rolling_expression_metric.yml
rolling_expression_metric_yml = """
version: 2 
models:
  - name: rolling_expression_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('rolling_expression_metric__expected')
metrics:
  - name: rolling_expression_metric
    label: Expression ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: expression
    sql: "{{metric('base_sum_metric')}} + 1"
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_expression_metric__expected.csv
if os.getenv('dbt_target') == 'postgres':
    rolling_expression_metric__expected_csv = """
date_month,base_sum_metric,rolling_expression_metric,rolling_expression_metric_rolling_max_2_month,rolling_expression_metric_rolling_min_2_month,rolling_expression_metric_rolling_sum_2_month
2022-01-01,8,9,9,9,9
2022-02-01,6,7,9,7,16
""".lstrip()

# seeds/rolling_expression_metric__expected.csv
if os.getenv('dbt_target') == 'redshift':
    rolling_expression_metric__expected_csv = """
date_month,base_sum_metric,rolling_expression_metric,rolling_expression_metric_rolling_max_2_month,rolling_expression_metric_rolling_min_2_month,rolling_expression_metric_rolling_sum_2_month
2022-01-01,8,9,9,9,9
2022-02-01,6,7,9,7,16
""".lstrip()

# seeds/rolling_expression_metric__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    rolling_expression_metric__expected_csv = """
date_month,base_sum_metric,rolling_expression_metric,rolling_expression_metric_rolling_max_2_month,rolling_expression_metric_rolling_min_2_month,rolling_expression_metric_rolling_sum_2_month
2022-01-01,8,9,9,9,9
2022-02-01,6,7,9,7,16
""".lstrip()

# seeds/rolling_expression_metric__expected.csv
if os.getenv('dbt_target') == 'bigquery':
    rolling_expression_metric__expected_csv = """
date_month,base_sum_metric,rolling_expression_metric,rolling_expression_metric_rolling_max_2_month,rolling_expression_metric_rolling_min_2_month,rolling_expression_metric_rolling_sum_2_month
2022-01-01,8,9,9,9,9
2022-02-01,6,7,9,7,16
""".lstrip()

# seeds/rolling_expression__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_expression__expected_yml = """
version: 2
seeds:
  - name: rolling_expression__expected
    config:
      column_types:
        date_month: date
        rolling_expression: INT64
        rolling_expression_rolling_min_2_month: INT64
        rolling_expression_rolling_max_2_month: INT64
        rolling_expression_rolling_sum_2_month: INT64
""".lstrip()
else: 
    rolling_expression__expected_yml = """"""

class TestRollingExpressionMetric:

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
            "rolling_expression_metric__expected.csv": rolling_expression_metric__expected_csv,
            "rolling_expression__expected.yml": rolling_expression__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "base_sum_metric.yml": base_sum_metric_yml,
            "rolling_expression_metric.yml": rolling_expression_metric_yml,
            "fact_orders.sql": fact_orders_sql,
            "rolling_expression_metric.sql": rolling_expression_metric_sql
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