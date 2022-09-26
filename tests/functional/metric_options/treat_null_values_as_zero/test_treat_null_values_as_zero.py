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

# models/treat_null_values_as_zero.sql
treat_null_values_as_zero_sql = """
select *
from 
{{ metrics.calculate(metric('treat_null_values_as_zero'), 
    grain='month',
    dimensions=['had_discount','order_country']
    )
}}
"""

# models/treat_null_values_as_zero.yml
treat_null_values_as_zero_yml = """
version: 2 

models:
  - name: treat_null_values_as_zero
    tests: 
      - dbt_utils.equality:
          compare_model: ref('treat_null_values_as_zero__expected')
metrics:
  - name: treat_null_values_as_zero
    model: ref('fact_orders')
    label: Total Amount (Nulls)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country
    config:
      treat_null_values_as_zero: false
"""

# seeds/treat_null_values_as_zero__expected.csv
treat_null_values_as_zero__expected_csv = """
date_month,had_discount,order_country,treat_null_values_as_zero
2022-01-01,TRUE,France,1
2022-01-01,TRUE,Japan,1
2022-01-01,FALSE,France,4
2022-01-01,FALSE,Japan,2
2022-02-01,TRUE,France,4
2022-02-01,FALSE,France,
2022-02-01,FALSE,Japan,2
2022-02-01,TRUE,Japan,
""".lstrip()

class TestDefaultValueNullMetric:

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
            "treat_null_values_as_zero__expected.csv": treat_null_values_as_zero__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "treat_null_values_as_zero.yml": treat_null_values_as_zero_yml,
            "fact_orders.sql": fact_orders_sql,
            "treat_null_values_as_zero.sql": treat_null_values_as_zero_sql
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

        # # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]