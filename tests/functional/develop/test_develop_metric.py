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

# models/develop_metric.sql
develop_metric_sql = """
{% set my_metric_yml -%}

metrics:
  - name: develop_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: average
    sql: discount_total
    dimensions:
      - had_discount
      - order_country

{%- endset %}

select * 
from {{ dbt_metrics.develop(
        develop_yml=my_metric_yml,
        grain='month'
        )
    }}
"""

# models/develop_metric.yml
develop_metric_yml = """
version: 2 
models:
  - name: develop_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('develop_metric__expected')

"""

# seeds/develop_metric__expected.csv
if os.getenv('dbt_target') == 'postgres':
    develop_metric__expected_csv = """
date_month,develop_metric
2022-01-01,1.00000000000000000000
2022-02-01,1.3333333333333333
""".lstrip()

# seeds/develop_metric__expected.csv
if os.getenv('dbt_target') == 'redshift':
    develop_metric__expected_csv = """
date_month,develop_metric
2022-01-01,1.00000000000000000000
2022-02-01,1.3333333333333333
""".lstrip()

# seeds/develop_metric__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    develop_metric__expected_csv = """
date_month,develop_metric
2022-01-01,1.000000
2022-02-01,1.333333
""".lstrip()

# seeds/develop_metric__expected.csv
if os.getenv('dbt_target') == 'bigquery':
    develop_metric__expected_csv = """
date_month,develop_metric
2022-01-01,1.00000000000000000000
2022-02-01,1.3333333333333333
""".lstrip()

# seeds/develop_metric___expected.yml
if os.getenv('dbt_target') == 'bigquery':
    develop_metric__expected_yml = """
version: 2
seeds:
  - name: develop_metric__expected
    config:
      column_types:
        date_month: date
        develop_metric: FLOAT64
""".lstrip()
else: 
    develop_metric__expected_yml = """"""

class TestDevelopMonthlyMetric:
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
            "fact_orders_source.csv": fact_orders_source_csv,
            "develop_metric__expected.csv": develop_metric__expected_csv,
            "develop_metric__expected.yml": develop_metric__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "develop_metric.sql": develop_metric_sql,
            "develop_metric.yml": develop_metric_yml
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 2

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 3

        # test tests
        results = run_dbt(["test"]) # expect passing test
        assert len(results) == 1

        # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]