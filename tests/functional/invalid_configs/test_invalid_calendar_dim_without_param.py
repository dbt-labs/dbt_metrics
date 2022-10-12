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

# models/custom_calendar.sql
custom_calendar_sql = """
with days as (
    {{ metrics.metric_date_spine(
    datepart="day",
    start_date="cast('2010-01-01' as date)",
    end_date="cast('2030-01-01' as date)"
   )
    }}
),

final as (
    select 
        cast(date_day as date) as date_day,
        {% if target.type == 'bigquery' %}
            --BQ starts its weeks on Sunday. I don't actually care which day it runs on for auto testing purposes, just want it to be consistent with the other seeds
            cast({{ date_trunc('week(MONDAY)', 'date_day') }} as date) as date_week,
        {% else %}
            cast({{ date_trunc('week', 'date_day') }} as date) as date_week,
        {% endif %}
        cast({{ date_trunc('month', 'date_day') }} as date) as date_month,
        cast({{ date_trunc('quarter', 'date_day') }} as date) as date_quarter,
        cast({{ date_trunc('year', 'date_day') }} as date) as date_year,
        true as is_weekend
    from days
)

select * from final

"""


# models/base_sum_metric.sql
base_sum_metric_sql = """
select *
from 
{{ metrics.calculate(metric('base_sum_metric'), 
    grain='month',
    dimensions=["is_not_weekend"]
    )
}}
"""

# models/base_sum_metric.yml
base_sum_metric_yml = """
version: 2 
models:
  - name: base_sum_metric
    tests: 
      - metrics.metric_equality:
          compare_model: ref('base_sum_metric__expected')
metrics:
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

class TestInvalidCustomCalendarDimensionsMetric:

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
            "fact_orders_source.csv": fact_orders_source_csv
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "custom_calendar.sql": custom_calendar_sql,
            "base_sum_metric.sql": base_sum_metric_sql,
            "base_sum_metric.yml": base_sum_metric_yml
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])
        results = run_dbt(["seed"])

        # initial run
        results = run_dbt(["run"],expect_pass = False)