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

# models/invalid_calendar_dim_without_param.sql
invalid_calendar_dim_without_param_sql = """
select *
from 
{{ metrics.calculate(metric('invalid_calendar_dim_without_param'), 
    grain='month',
    dimensions=['date_year']
    )
}}
"""

# models/invalid_calendar_dim_without_param.yml
invalid_calendar_dim_without_param_yml = """
version: 2 
models:
  - name: invalid_calendar_dim_without_param
    tests: 
      - dbt_utils.equality:
          compare_model: ref('invalid_calendar_dim_without_param__expected')
metrics:
  - name: invalid_calendar_dim_without_param
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


class TestInvalidCalendarDimensionWithoutParameter:

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
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "invalid_calendar_dim_without_param.sql": invalid_calendar_dim_without_param_sql,
            "invalid_calendar_dim_without_param.yml": invalid_calendar_dim_without_param_yml
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])
        results = run_dbt(["seed"])

        # initial run
        results = run_dbt(["run"],expect_pass = False)