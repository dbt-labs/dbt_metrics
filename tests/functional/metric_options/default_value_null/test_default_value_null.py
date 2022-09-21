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

# models/default_value_null.sql
default_value_null_sql = """
select *
from 
{{ metrics.calculate(metric('default_value_null'), 
    grain='month',
    dimensions=['had_discount','order_country']
    )
}}
"""

# models/default_value_null.yml
default_value_null_yml = """
version: 2 

models:
  - name: default_value_null
    tests: 
      - dbt_utils.equality:
          compare_model: ref('default_value_null__expected')
metrics:
  - name: default_value_null
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
      default_value_null: true
"""

# seeds/default_value_null__expected.csv
default_value_null__expected_csv = """
date_month,had_discount,order_country,default_value_null
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
            "default_value_null__expected.csv": default_value_null__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "default_value_null.yml": default_value_null_yml,
            "fact_orders.sql": fact_orders_sql,
            "default_value_null.sql": default_value_null_sql
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