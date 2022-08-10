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
backwards_compatability_metric_sql = """
select *
from 
{{ metrics.metric(
    metric_name='backwards_compatability_metric', 
    grain='month'
    )
}}
"""

# models/backwards_compatability_metric.yml
backwards_compatability_metric_yml = """
version: 2 
models:
  - name: backwards_compatability_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('backwards_compatability_metric__expected')
metrics:
  - name: backwards_compatability_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: average
    sql: discount_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/backwards_compatability_metric__expected.csv
if os.getenv('dbt_target') == 'postgres':
    backwards_compatability_metric__expected_csv = """
date_month,backwards_compatability_metric
2022-01-01,1.00000000000000000000
2022-02-01,1.3333333333333333
""".lstrip()

# seeds/backwards_compatability_metric__expected.csv
if os.getenv('dbt_target') == 'redshift':
    backwards_compatability_metric__expected_csv = """
date_month,backwards_compatability_metric
2022-01-01,1.00000000000000000000
2022-02-01,1.3333333333333333
""".lstrip()

# seeds/backwards_compatability_metric__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    backwards_compatability_metric__expected_csv = """
date_month,backwards_compatability_metric
2022-01-01,1.000000
2022-02-01,1.333333
""".lstrip()

# seeds/backwards_compatability_metric__expected.csv
if os.getenv('dbt_target') == 'bigquery':
    backwards_compatability_metric__expected_csv = """
date_month,backwards_compatability_metric
2022-01-01,1.00000000000000000000
2022-02-01,1.3333333333333333
""".lstrip()

# seeds/backwards_compatability_metric___expected.yml
if os.getenv('dbt_target') == 'bigquery':
    backwards_compatability_metric__expected_yml = """
version: 2
seeds:
  - name: backwards_compatability_metric__expected
    config:
      column_types:
        date_month: date
        backwards_compatability_metric: FLOAT64
""".lstrip()
else: 
    backwards_compatability_metric__expected_yml = """"""

class TestBackwardsCompatibility:
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
            "backwards_compatability_metric__expected.csv": backwards_compatability_metric__expected_csv,
            "backwards_compatability_metric__expected.yml": backwards_compatability_metric__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "backwards_compatability_metric.sql": backwards_compatability_metric_sql,
            "backwards_compatability_metric.yml": backwards_compatability_metric_yml
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