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

# models/ratio_metric.sql
ratio_metric_sql = """
select *
from 
{{ metrics.calculate(metric('ratio_metric'), 
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

# models/base_average_metric.yml
base_average_metric_yml = """
version: 2 
metrics:
  - name: base_average_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: average
    sql: discount_total
    dimensions:
      - had_discount
      - order_country     
"""

# models/ratio_metric.yml
ratio_metric_yml = """
version: 2 
models:
  - name: ratio_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('ratio_metric__expected')
metrics:
  - name: ratio_metric
    label: Ratio ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: expression
    sql: "{{metric('base_sum_metric')}} / {{metric('base_average_metric')}}"
    dimensions:
      - had_discount
      - order_country
"""

# seeds/ratio_metric__expected.csv
ratio_metric__expected_csv = """
date_month,base_sum_metric,base_average_metric,ratio_metric
2022-02-01,2,1.00000000000000000000,2.00000000000000000000
2022-01-01,8,1.00000000000000000000,8.00000000000000000000
""".lstrip()

class TestRatioMetric:

    # configuration in dbt_project.yml
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
            "ratio_metric__expected.csv": ratio_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "base_average_metric.yml": base_average_metric_yml,
            "base_sum_metric.yml": base_sum_metric_yml,
            "ratio_metric.yml": ratio_metric_yml,
            "fact_orders.sql": fact_orders_sql,
            "ratio_metric.sql": ratio_metric_sql
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
        # result_statuses = sorted(r.status for r in results)
        # assert result_statuses == ["pass"]