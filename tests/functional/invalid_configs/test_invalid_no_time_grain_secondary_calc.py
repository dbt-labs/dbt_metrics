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

# models/no_time_grain_base_sum_metric.sql
no_time_grain_base_sum_metric_sql = """
select *
from 
{{ metrics.calculate(
    metric('no_time_grain_base_sum_metric'), 
    secondary_calculations=[
        metrics.period_over_period(comparison_strategy="difference", interval=1, alias = "1mth")
    ]
    )
}}
"""

# models/no_time_grain_base_sum_metric.yml
no_time_grain_base_sum_metric_yml = """
version: 2 
metrics:
  - name: no_time_grain_base_sum_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

class TestNoTimeGrainSecondaryCalcMetric:

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {"+materialized": "table"},
            "vars":{
                "dbt_metrics_calendar_model": "custom_calendar",
                "custom_calendar_dimension_list": ["is_weekend"]
            }
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
            "custom_calendar.sql": custom_calendar_sql,
            "no_time_grain_base_sum_metric.sql": no_time_grain_base_sum_metric_sql,
            "no_time_grain_base_sum_metric.yml": no_time_grain_base_sum_metric_yml
        }

    def test_invalid_no_time_grain_secondary_calc(self,project,):
        # initial run
        run_dbt(["deps"])
        run_dbt(["seed"])
        run_dbt(["run"],expect_pass = False)

