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
    window: 14 seconds
    sql: discount_total
    dimensions:
      - had_discount
      - order_country
"""

class TestInvalidWindowPeriod:

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
            "fact_orders_source.csv": fact_orders_source_csv
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
        results = run_dbt(["seed"])

        # initial run
        results = run_dbt(["run"],expect_pass = False)