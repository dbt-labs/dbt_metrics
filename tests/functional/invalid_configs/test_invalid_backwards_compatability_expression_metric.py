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

# models/backwards_compatability_metric.sql
backwards_compatability_derived_metric_sql = """
select *
from 
{{ metrics.metric(
    metric_name='backwards_compatability_derived_metric', 
    grain='month'
    )
}}
"""

# models/backwards_compatability_metric.yml
backwards_compatability_metric_yml = """
version: 2 
models:
  - name: backwards_compatability_derived_metric

metrics:
  - name: backwards_compatability_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: discount_total
    dimensions:
      - had_discount
      - order_country

  - name: backwards_compatability_derived_metric
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{metric('backwards_compatability_metric')}} + 1"
    dimensions:
      - had_discount
      - order_country
"""

class TestBackwardsCompatibilityDerivedMetric:
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
            "backwards_compatability_derived_metric.sql": backwards_compatability_derived_metric_sql,
            "backwards_compatability_metric.yml": backwards_compatability_metric_yml
        }

    def test_build_completion(self,project,):
        results = run_dbt(["deps"])
        results = run_dbt(["seed"])

        # initial run
        results = run_dbt(["run"],expect_pass = False)
