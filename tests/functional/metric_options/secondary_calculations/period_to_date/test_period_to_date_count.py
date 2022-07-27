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

# models/period_to_date_count.sql
period_to_date_count_sql = """
select *
from 
{{ metrics.calculate(metric('period_to_date_count'), 
    grain='month',
    secondary_calculations=[
        metrics.period_to_date(aggregate="min", period="year", alias="this_year_min"),
        metrics.period_to_date(aggregate="max", period="year"),
        metrics.period_to_date(aggregate="sum", period="year"),
        metrics.period_to_date(aggregate="average", period="year"),
    ]
    )
}}
"""

# models/period_to_date_count.yml
period_to_date_count_yml = """
version: 2 
models:
  - name: period_to_date_count
    tests: 
      - dbt_utils.equality:
          compare_model: ref('period_to_date_count__expected')
metrics:
  - name: period_to_date_count
    model: ref('fact_orders')
    label: Count Distinct
    timestamp: order_date
    time_grains: [day, week, month]
    type: count
    sql: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/period_to_date_count__expected.csv
period_to_date_count__expected_csv = """
date_month,date_year,period_to_date_count,period_to_date_count_this_year_min,period_to_date_count_max_for_year,period_to_date_count_sum_for_year,period_to_date_count_average_for_year
2022-01-01,2022-01-01,7,7,7,7,7.0000000000000000
2022-02-01,2022-01-01,3,3,7,10,5.0000000000000000
""".lstrip()

class TestPeriodToDateCount:

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
            "period_to_date_count__expected.csv": period_to_date_count__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "period_to_date_count.sql": period_to_date_count_sql,
            "period_to_date_count.yml": period_to_date_count_yml
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