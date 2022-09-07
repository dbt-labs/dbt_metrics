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

# models/base_window_metric.sql
base_window_metric_sql = """
select *
from 
{{ metrics.calculate(metric('base_window_metric'), 
    grain='week'
    )
}}
"""

# models/base_window_metric.yml
base_window_metric_yml = """
version: 2 
models:
  - name: base_window_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('base_window_metric__expected')
metrics:
  - name: base_window_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: sum
    window: 1 month
    sql: discount_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/base_window_metric__expected.csv
base_window_metric__expected_csv = """
date_week,base_window_metric
2022-01-10,2
2022-01-17,3
2022-01-24,6
2022-01-31,7
2022-02-07,7
2022-02-14,6
2022-02-21,7
2022-02-28,5
2022-03-14,2
2022-03-07,3
""".lstrip()

class TestBaseMonthWindowMetric:

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
            "base_window_metric__expected.csv": base_window_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "base_window_metric.sql": base_window_metric_sql,
            "base_window_metric.yml": base_window_metric_yml
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