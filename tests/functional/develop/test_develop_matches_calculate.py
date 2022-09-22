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

# models/testing_metric__develop.sql
testing_metric_develop_sql = """
{% set my_metric_yml -%}

metrics:
  - name: testing_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: discount_total
    dimensions:
      - had_discount
      - order_country

{%- endset %}

select * 
from {{ metrics.develop(
        develop_yml=my_metric_yml,
        metric_list='testing_metric',
        grain='month'
        )
    }}
"""

# models/testing_metric_calculate.yml
testing_metric_calculate_yml = """
version: 2 

metrics:
  - name: testing_metric
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

# models/testing_metric_calculate.sql
testing_metric_calculate_sql = """
select *
from 
{{ metrics.calculate(metric('testing_metric'), 
    grain='month'
    )
}}
"""

# models/testing_metric_develop.yml
testing_metric_develop_yml = """
version: 2 
models:
  - name: testing_metric_develop
    tests: 
      - dbt_utils.equality:
          compare_model: ref('testing_metric_calculate')

"""

class TestDevelopMatchesCalculate:
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
            "testing_metric_develop.sql": testing_metric_develop_sql,
            "testing_metric_develop.yml": testing_metric_develop_yml,
            "testing_metric_calculate.sql": testing_metric_calculate_sql,
            "testing_metric_calculate.yml": testing_metric_calculate_yml
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 1

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 4

        # test tests
        results = run_dbt(["test"]) # expect passing test
        assert len(results) == 1

        # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]