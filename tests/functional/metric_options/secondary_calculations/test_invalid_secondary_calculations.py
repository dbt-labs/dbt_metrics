from struct import pack
import os
import pytest
from dbt.tests.util import run_dbt

# our file contents
from tests.functional.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml,
    custom_calendar_sql

)

# models/avg_metric.sql
avg_metric_sql = """
select *
from 
{{ metrics.calculate(metric('avg_metric'), 
    grain='week',
    secondary_calculations=[metrics.rolling(aggregate="sum",interval=2)]
    )
}}
"""

# models/avg_metric.yml
avg_metric_yml = """
version: 2 
models:
  - name: avg_metric

metrics:
  - name: avg_metric
    model: ref('fact_orders')
    label: Count Distinct
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

class TestInvalidAverageSecondaryCalc:

    # configuration in dbt_project.yml
    # setting bigquery as table to get around query complexity 
    # resource constraints with compunding views
    if os.getenv('dbt_target') == 'bigquery':
        @pytest.fixture(scope="class")
        def project_config_update(self):
            return {
            "name": "example",
            "models": {"+materialized": "table"},
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
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "avg_metric.sql": avg_metric_sql,
            "avg_metric.yml": avg_metric_yml
        }

    def test_invalid_average_metric(self,project,):
        # running deps to install package
        run_dbt(["deps"])
        # seed seeds
        run_dbt(["seed"])
        # initial run
        run_dbt(["run"], expect_pass = False)


# models/median_metric.sql
median_metric_sql = """
select *
from 
{{ metrics.calculate(metric('median_metric'), 
    grain='week',
    secondary_calculations=[
        metrics.rolling(aggregate="sum",interval=2)
    ]
    )
}}
"""

# models/median_metric.yml
median_metric_yml = """
version: 2 
models:
  - name: median_metric

metrics:
  - name: median_metric
    model: ref('fact_orders')
    label: Count Distinct
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: median
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

class TestInvalidMedianSecondaryCalc:

    # configuration in dbt_project.yml
    # setting bigquery as table to get around query complexity 
    # resource constraints with compunding views
    if os.getenv('dbt_target') == 'bigquery':
        @pytest.fixture(scope="class")
        def project_config_update(self):
            return {
            "name": "example",
            "models": {"+materialized": "table"},
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
            "fact_orders_source.csv": fact_orders_source_csv
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "median_metric.sql": median_metric_sql,
            "median_metric.yml": median_metric_yml
        }

    def test_invalid_median_metric(self,project,):
        # running deps to install package
        run_dbt(["deps"])
        # seed seeds
        run_dbt(["seed"])
        # initial run
        run_dbt(["run"], expect_pass = False)