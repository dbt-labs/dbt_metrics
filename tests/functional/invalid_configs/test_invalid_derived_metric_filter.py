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

# models/invalid_derived_metric.sql
invalid_derived_metric_sql = """
select *
from 
{{ metrics.calculate(metric('invalid_derived_metric'), 
    grain='month'
    )
}}
"""

# models/invalid_derived_metric.yml
invalid_derived_metric_yml = """
version: 2 
models:
  - name: invalid_derived_metric

metrics:
  - name: base_sum_metric
    model: ref('fact_orders')
    label: Order Total ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country

  - name: invalid_derived_metric
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{metric('base_sum_metric')}} - 1"
    dimensions:
      - had_discount
      - order_country

    filters:
      - field: had_discount
        operator: 'is'
        value: 'true'
"""

class TestInvalidDerivedMetric:

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
            "invalid_derived_metric.sql": invalid_derived_metric_sql,
            "invalid_derived_metric.yml": invalid_derived_metric_yml
        }

    def test_invalid_derived_metric(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Here we expect the run to fail because the incorrect
        # config won't allow it to compile
        run_dbt(["run"], expect_pass = False)