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

# models/period_over_period_difference.sql
period_over_period_difference_sql = """
select *
from 
{{ metrics.calculate(metric('period_over_period_difference'), 
    grain='month',
    secondary_calculations=[
        metrics.period_over_period(comparison_strategy="difference", interval=1, alias = "1mth")
    ]
    )
}}
"""

# models/period_over_period_difference.yml
period_over_period_difference_yml = """
version: 2 
models:
  - name: period_over_period_difference
    tests: 
      - dbt_utils.equality:
          compare_model: ref('period_over_period_difference__expected')
metrics:
  - name: period_over_period_difference
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

# seeds/period_over_period_difference__expected.csv
period_over_period_difference__expected_csv = """
date_month,period_over_period_difference,period_over_period_difference_1mth
2022-01-01,8,8
2022-02-01,6,-2
""".lstrip()

class TestPeriodOverPeriodDifference:

    # configudifferencen in dbt_project.yml
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
            "period_over_period_difference__expected.csv": period_over_period_difference__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "period_over_period_difference.sql": period_over_period_difference_sql,
            "period_over_period_difference.yml": period_over_period_difference_yml
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