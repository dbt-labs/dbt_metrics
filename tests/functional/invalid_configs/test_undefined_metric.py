from configparser import ParsingError
from struct import pack
import os
import pytest
from dbt.tests.util import run_dbt
from dbt.exceptions import CompilationException, ParsingException

# our file contents
from tests.functional.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml,
)

# models/undefined_metric.sql
undefined_metric_sql = """
select *
from 
{{ metrics.calculate(metric('undefined_metric'), 
    grain='month'
    )
}}
"""

# models/undefined_metric.yml
undefined_metric_yml = """
version: 2 
models:
  - name: undefined_metric

metrics:
  - name: not_undefined_metric
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

class TestUndefinedMetric:

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
            "undefined_metric.sql": undefined_metric_sql,
            "undefined_metric.yml": undefined_metric_yml
        }

    def test_undefined_metric(self,project,):
        results = run_dbt(["deps"])
        # Here we expect the run to fail because the macro is calling 
        # an undefined metric
        with pytest.raises(CompilationException):
            run_dbt(["seed"])
            run_dbt(["run"])