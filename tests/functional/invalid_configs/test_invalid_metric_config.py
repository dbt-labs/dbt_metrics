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

# models/invalid_metric_config.sql
invalid_metric_config_sql = """
select *
from 
{{ metrics.calculate(metric('invalid_metric_config'), 
    grain='month'
    )
}}
"""

# models/invalid_metric_config.yml
invalid_metric_config_yml = """
version: 2 
models:
  - name: invalid_metric_config

metrics:
  - name: invalid_metric_config
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count
    expression: order_total
    dimensions:
      - had_discount
      - order_country
    config:
      did_dave_write_this: true
"""

class TestInvalidMetricConfig:

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
            "invalid_metric_config.sql": invalid_metric_config_sql,
            "invalid_metric_config.yml": invalid_metric_config_yml
        }

    def test_metric_config(self,project,):
        # initial run
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Here we expect the run to fail because the value provided
        # in the where clause isn't included in the final dataset
        results = run_dbt(["run"], expect_pass = False)