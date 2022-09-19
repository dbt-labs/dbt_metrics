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

# models/max_date_invalid_datatype.sql
max_date_invalid_datatype_sql = """
select *
from 
{{ metrics.calculate(metric('max_date_invalid_datatype'), 
    grain='day'
    )
}}
"""

# models/invalid_metric_names.yml
invalid_metric_names_yml = """
version: 2 

metrics:
  - name: max_date_invalid_datatype
    model: ref('fact_orders')
    label: max_date_invalid_datatype
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: max
    expression: order_date
"""

class TestInvalidDatatypes:

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
            "max_date_invalid_datatype.sql": max_date_invalid_datatype_sql,
            "invalid_metric_names.yml": invalid_metric_names_yml
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])
        results = run_dbt(["seed"])

        # initial run
        results = run_dbt(["run"],expect_pass = False)