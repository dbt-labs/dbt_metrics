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

# models/case_when_metric.sql
case_when_metric_sql = """
select *
from 
{{ metrics.calculate(metric('case_when_metric'), 
    grain='month'
    )
}}
"""

# models/case_when_metric.yml
case_when_metric_yml = """
version: 2 
models:
  - name: case_when_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('case_when_metric__expected')
metrics:
  - name: case_when_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: sum
    sql: case when had_discount = true then 1 else 0 end 
    dimensions:
      - order_country
"""

# seeds/case_when_metric__expected.csv
case_when_metric__expected_csv = """
date_month,case_when_metric
2022-01-01,2
2022-02-01,1
""".lstrip()

class TestCaseWhenMetric:

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
            "case_when_metric__expected.csv": case_when_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "case_when_metric.sql": case_when_metric_sql,
            "case_when_metric.yml": case_when_metric_yml
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