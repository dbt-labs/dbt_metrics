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

# models/invalid_dimension.sql
invalid_dimension_sql = """
select *
from 
{{ metrics.calculate(metric('invalid_dimension'), 
    grain='year',
    dimensions=['invalid_dimension_name']
    )
}}
"""

# models/multiple_metrics.sql
multiple_metrics_sql = """
select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric'), metric('base_count_metric')],
    grain='month',
    dimensions=['invalid_dimension_name']
    )
}}
"""

# models/invalid_dimension.yml
invalid_dimension_yml = """
version: 2 
models:
  - name: invalid_dimension

metrics:
  - name: invalid_dimension
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

# models/multiple_metrics.yml
multiple_metrics_yml = """
version: 2 
models:
  - name: multiple_metrics
    tests: 
      - dbt_utils.equality:
          compare_model: ref('multiple_metrics__expected')
metrics:
  - name: base_count_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count
    expression: order_total
    dimensions:
      - had_discount
      - order_country

  - name: base_sum_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

class TestInvalidDimension:

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
            "invalid_dimension.sql": invalid_dimension_sql,
            "invalid_dimension.yml": invalid_dimension_yml,
            "multiple_metrics.yml": multiple_metrics_yml,
            "multiple_metrics.sql": multiple_metrics_sql
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Here we expect the run to fail because the incorrect
        # time grain won't allow it to compile
        results = run_dbt(["run"], expect_pass = False)