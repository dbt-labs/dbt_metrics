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

# models/base_max_metric.sql
base_max_metric_sql = """
select *
from 
{{ metrics.calculate(metric('base_max_metric'), 
    grain='month'
    )
}}
"""

# models/base_max_metric.yml
base_max_metric_yml = """
version: 2 
models:
  - name: base_max_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('base_max_metric__expected')
metrics:
  - name: base_max_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: max
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/base_max_metric__expected.csv
base_max_metric__expected_csv = """
date_month,base_max_metric
2022-01-01,2
2022-02-01,4
""".lstrip()

class TestBaseMaxMetric:

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
            "base_max_metric__expected.csv": base_max_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "base_max_metric.sql": base_max_metric_sql,
            "base_max_metric.yml": base_max_metric_yml
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