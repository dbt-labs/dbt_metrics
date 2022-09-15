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

# models/invalid_period_to_date_average.sql
invalid_period_to_date_average_sql = """
select *
from 
{{ metrics.calculate(metric('invalid_period_to_date_average'), 
    grain='month',
    secondary_calculations=[
        metrics.period_to_date(aggregate="average", period="year", alias="this_year_average")
    ]
    )
}}
"""

# models/invalid_period_to_date_average.yml
invalid_period_to_date_average_yml = """
version: 2 
models:
  - name: invalid_period_to_date_average
    tests: 
      - dbt_utils.equality:
          compare_model: ref('invalid_period_to_date_average__expected')
metrics:
  - name: invalid_period_to_date_average
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: discount_total
    dimensions:
      - had_discount
      - order_country
"""

class TestInvalidPeriodToDateAverage:

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
            "invalid_period_to_date_average.sql": invalid_period_to_date_average_sql,
            "invalid_period_to_date_average.yml": invalid_period_to_date_average_yml
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 1

        # initial run
        results = run_dbt(["run"], expect_pass = False)
