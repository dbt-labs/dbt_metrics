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

# models/period_to_date_max.sql
period_to_date_max_sql = """
select *
from 
{{ dbt_metrics.calculate(metric('period_to_date_max'), 
    grain='month',
    secondary_calculations=[
        dbt_metrics.period_to_date(aggregate="min", period="year", alias="this_year_min"),
        dbt_metrics.period_to_date(aggregate="max", period="year"),
        dbt_metrics.period_to_date(aggregate="sum", period="year"),
        dbt_metrics.period_to_date(aggregate="average", period="year"),
    ]
    )
}}
"""

# models/period_to_date_max.yml
period_to_date_max_yml = """
version: 2 
models:
  - name: period_to_date_max
    tests: 
      - dbt_utils.equality:
          compare_model: ref('period_to_date_max__expected')
metrics:
  - name: period_to_date_max
    model: ref('fact_orders')
    label: max value
    timestamp: order_date
    time_grains: [day, week, month]
    type: max
    sql: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/period_to_date_max__expected.csv
if os.getenv('dbt_target') == 'postgres':
    period_to_date_max__expected_csv = """
date_month,date_year,period_to_date_max,period_to_date_max_this_year_min,period_to_date_max_max_for_year,period_to_date_max_sum_for_year,period_to_date_max_average_for_year
2022-01-01,2022-01-01,5,5,5,5,5.0000000000000000
2022-02-01,2022-01-01,3,3,5,8,4.0000000000000000
""".lstrip()

# seeds/period_to_date_max__expected.csv
if os.getenv('dbt_target') == 'redshift':
    period_to_date_max__expected_csv = """
date_month,date_year,period_to_date_max,period_to_date_max_this_year_min,period_to_date_max_max_for_year,period_to_date_max_sum_for_year,period_to_date_max_average_for_year
2022-01-01,2022-01-01,5,5,5,5,5.0000000000000000
2022-02-01,2022-01-01,3,3,5,8,4.0000000000000000
""".lstrip()

# seeds/period_to_date_max__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    period_to_date_max__expected_csv = """
date_month,date_year,period_to_date_max,period_to_date_max_this_year_min,period_to_date_max_max_for_year,period_to_date_max_sum_for_year,period_to_date_max_average_for_year
2022-01-01,2022-01-01,5,5,5,5,5.000000
2022-02-01,2022-01-01,3,3,5,8,4.000000
""".lstrip()

# seeds/period_to_date_max__expected.csv
if os.getenv('dbt_target') == 'bigquery':
    period_to_date_max__expected_csv = """
date_month,date_year,period_to_date_max,period_to_date_max_this_year_min,period_to_date_max_max_for_year,period_to_date_max_sum_for_year,period_to_date_max_average_for_year
2022-01-01,2022-01-01,5,5,5,5,5.0000000000000000
2022-02-01,2022-01-01,3,3,5,8,4.0000000000000000
""".lstrip()

# seeds/period_to_date_max__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    period_to_date_max__expected_yml = """
version: 2
seeds:
  - name: period_to_date_max__expected
    config:
      column_types:
        date_month: date
        date_year: date
        period_to_date_max: INT64
        period_to_date_max_this_year_min: INT64
        period_to_date_max_max_for_year: INT64
        period_to_date_max_sum_for_year: INT64
        period_to_date_max_average_for_year: FLOAT64
""".lstrip()
else: 
    period_to_date_max__expected_yml = """"""

class TestPeriodToDateMax:

    # configuration in dbt_project.yml
    # setting bigquery as table to get around query complexity 
    # resource constraints with compunding views
    if os.getenv('dbt_target') == 'bigquery':
        @pytest.fixture(scope="class")
        def project_config_update(self):
            return {
            "name": "example",
            "models": {"+materialized": "table"}
            }
    else: 
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
            "period_to_date_max__expected.csv": period_to_date_max__expected_csv,
            "period_to_date_max__expected.yml": period_to_date_max__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "period_to_date_max.sql": period_to_date_max_sql,
            "period_to_date_max.yml": period_to_date_max_yml
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