import os
import pytest
from dbt.tests.util import run_dbt

# our file contents
from tests.functional.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml,
    BaseTestSetup,
)

# models/base_average_metric.sql
base_average_metric_sql = """
select *
from 
{{ metrics.calculate(metric('base_average_metric'), 
    grain='month'
    )
}}
"""

# models/base_average_metric.yml
base_average_metric_yml = """
version: 2 
models:
  - name: base_average_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('base_average_metric__expected')
metrics:
  - name: base_average_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: discount_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/base_average_metric__expected.csv
if os.getenv('dbt_target') != 'snowflake':
    base_average_metric__expected_csv = """
date_month,base_average_metric
2022-01-01,1.00000000000000000000
2022-02-01,1.3333333333333333
""".lstrip()
else:
    base_average_metric__expected_csv = """
date_month,base_average_metric
2022-01-01,1.000000
2022-02-01,1.333333
""".lstrip()


# seeds/base_average_metric___expected.yml
if os.getenv('dbt_target') == 'bigquery':
    base_average_metric__expected_yml = """
version: 2
seeds:
  - name: base_average_metric__expected
    config:
      column_types:
        date_month: date
        base_average_metric: FLOAT64
""".lstrip()
else: 
    base_average_metric__expected_yml = """"""

class TestBaseAverageMetric(BaseTestSetup):

    SEEDS_DICT = {
            "fact_orders_source.csv": fact_orders_source_csv,
            "base_average_metric__expected.csv": base_average_metric__expected_csv,
            "base_average_metric__expected.yml": base_average_metric__expected_yml
        }
    
    MODELS_DICT = {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "base_average_metric.sql": base_average_metric_sql,
            "base_average_metric.yml": base_average_metric_yml
        }

    def test_base_average_metric(self, project):
        self.simple_build(project)