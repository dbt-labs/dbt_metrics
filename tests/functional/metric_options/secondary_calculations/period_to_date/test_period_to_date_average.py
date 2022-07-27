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

# models/period_to_date_average.sql
period_to_date_average_sql = """
select *
from 
{{ metrics.calculate(metric('period_to_date_average'), 
    grain='month',
    secondary_calculations=[
        metrics.period_to_date(aggregate="min", period="year", alias="this_year_min"),
        metrics.period_to_date(aggregate="max", period="year"),
    ]
    )
}}
"""

# models/period_to_date_average.yml
period_to_date_average_yml = """
version: 2 
models:
  - name: period_to_date_average
    tests: 
      - dbt_utils.equality:
          compare_model: ref('period_to_date_average__expected')
metrics:
  - name: period_to_date_average
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

# seeds/period_to_date_average__expected.csv
period_to_date_average__expected_csv = """
date_month,date_year,period_to_date_average,period_to_date_average_this_year_min,period_to_date_average_max_for_year
2022-01-01,2022-01-01,1.00000000000000000000,1,1
2022-02-01,2022-01-01,1.3333333333333333,1,1.3333333333333333
""".lstrip()

class TestPeriodToDateAverage:

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
            "period_to_date_average__expected.csv": period_to_date_average__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "period_to_date_average.sql": period_to_date_average_sql,
            "period_to_date_average.yml": period_to_date_average_yml
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