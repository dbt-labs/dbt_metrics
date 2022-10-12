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

# models/develop_metric_dimension.sql
develop_metric_dimension_sql = """
{% set my_metric_yml -%}

metrics:
  - name: develop_metric_dimension
    model: ref('fact_orders')
    label: develop metric dimensions
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country

{%- endset %}

select * 
from {{ metrics.develop(
        develop_yml=my_metric_yml,
        metric_list='develop_metric_dimension',
        grain='month',
        dimensions=['had_discount','order_country']
        )
    }}
"""

# models/develop_metric_dimension.yml
develop_metric_dimension_yml = """
version: 2 
models:
  - name: develop_metric_dimension
    tests: 
      - metrics.metric_equality:
          compare_model: ref('develop_metric_dimension__expected')

"""

# seeds/develop_metric_dimension__expected.csv
develop_metric_dimension__expected_csv = """
date_month,had_discount,order_country,develop_metric_dimension
2022-01-01,TRUE,France,1
2022-01-01,TRUE,Japan,1
2022-01-01,FALSE,France,4
2022-01-01,FALSE,Japan,2
2022-02-01,TRUE,France,4
2022-02-01,FALSE,France,0
2022-02-01,FALSE,Japan,2
2022-02-01,TRUE,Japan,0
""".lstrip()

class TestDevelopMetricDimension:
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
            "develop_metric_dimension__expected.csv": develop_metric_dimension__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "develop_metric_dimension.sql": develop_metric_dimension_sql,
            "develop_metric_dimension.yml": develop_metric_dimension_yml
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