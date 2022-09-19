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

# models/invalid_metric_name.sql
invalid_metric_name_sql = """
select *
from 
{{ metrics.calculate(metric('invalid metric name'), 
    grain='month'
    )
}}
"""

# models/invalid_metric_name.yml
invalid_metric_name_yml = """
version: 2 
models:
  - name: invalid_metric_name

metrics:
  - name: invalid metric name
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

class TestInvalidMetricName:

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
            "invalid_metric_name.sql": invalid_metric_name_sql,
            "invalid_metric_name.yml": invalid_metric_name_yml
        }

    def test_model_name(self,project,):
        # initial run
        with pytest.raises(ParsingException):
            run_dbt(["deps"])
            run_dbt(["run"])