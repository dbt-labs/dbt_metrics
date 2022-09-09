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

# models/base_min_metric.sql
base_min_metric_sql = """
select *
from 
{{ dbt_metrics.calculate(metric('base_min_metric'), 
    grain='month'
    )
}}
"""

# models/base_min_metric.yml
base_min_metric_yml = """
version: 2 
models:
  - name: base_min_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('base_min_metric__expected')
metrics:
  - name: base_min_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: min
    sql: order_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/base_min_metric__expected.csv
base_min_metric__expected_csv = """
date_month,base_min_metric
2022-01-01,1
2022-02-01,1
""".lstrip()

class TestBaseMinMetric:

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
            "base_min_metric__expected.csv": base_min_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "base_min_metric.sql": base_min_metric_sql,
            "base_min_metric.yml": base_min_metric_yml
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