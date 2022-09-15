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

# models/rolling_sum.sql
rolling_sum_sql = """
select *
from 
{{ metrics.calculate(metric('rolling_sum'), 
    grain='month',
    secondary_calculations=[
        metrics.rolling(aggregate="min", interval=2),
        metrics.rolling(aggregate="max", interval=2),
        metrics.rolling(aggregate="sum", interval=2),
        metrics.rolling(aggregate="average", interval=2)
    ]
    )
}}
"""

# models/rolling_sum.yml
rolling_sum_yml = """
version: 2 
models:
  - name: rolling_sum
    tests: 
      - dbt_utils.equality:
          compare_model: ref('rolling_sum__expected')
metrics:
  - name: rolling_sum
    model: ref('fact_orders')
    label: Count Distinct
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_sum__expected.csv
if os.getenv('dbt_target') == 'postgres':
    rolling_sum__expected_csv = """
date_month,rolling_sum,rolling_sum_rolling_min_2_month,rolling_sum_rolling_max_2_month,rolling_sum_rolling_sum_2_month,rolling_sum_rolling_average_2_month
2022-01-01,18,18,18,18,18.0000000000000000
2022-02-01,6,6,18,24,12.0000000000000000
""".lstrip()

# seeds/rolling_sum__expected.csv
if os.getenv('dbt_target') == 'redshift':
    rolling_sum__expected_csv = """
date_month,rolling_sum,rolling_sum_rolling_min_2_month,rolling_sum_rolling_max_2_month,rolling_sum_rolling_sum_2_month,rolling_sum_rolling_average_2_month
2022-01-01,18,18,18,18,18.0000000000000000
2022-02-01,6,6,18,24,12.0000000000000000
""".lstrip()

# seeds/rolling_sum__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    rolling_sum__expected_csv = """
date_month,rolling_sum,rolling_sum_rolling_min_2_month,rolling_sum_rolling_max_2_month,rolling_sum_rolling_sum_2_month,rolling_sum_rolling_average_2_month
2022-01-01,18,18,18,18,18.000000
2022-02-01,6,6,18,24,12.000000
""".lstrip()

# seeds/rolling_sum__expected.csv
if os.getenv('dbt_target') == 'bigquery':
    rolling_sum__expected_csv = """
date_month,rolling_sum,rolling_sum_rolling_min_2_month,rolling_sum_rolling_max_2_month,rolling_sum_rolling_sum_2_month,rolling_sum_rolling_average_2_month
2022-01-01,18,18,18,18,18.0000000000000000
2022-02-01,6,6,18,24,12.0000000000000000
""".lstrip()

# seeds/rolling_sum__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_sum__expected_yml = """
version: 2
seeds:
  - name: rolling_sum__expected
    config:
      column_types:
        date_month: date
        rolling_sum: INT64
        rolling_sum_rolling_min_2_month: INT64
        rolling_sum_rolling_max_2_month: INT64
        rolling_sum_rolling_sum_2_month: INT64
        rolling_sum_rolling_average_2_month: FLOAT64
""".lstrip()
else: 
    rolling_sum__expected_yml = """"""

class TestRollingSum:

    # configuration in dbt_project.yml
    # setting bigquery as table to get around query complexity 
    # resource constraints with compunding views
    if os.getenv('dbt_target') == 'bigquery':
        @pytest.fixture(scope="class")
        def project_config_update(self):
            return {
            "name": "example",
            "models": {"+materialized": "table"}
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
            "rolling_sum__expected.csv": rolling_sum__expected_csv,
            "rolling_sum__expected.yml":rolling_sum__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "rolling_sum.sql": rolling_sum_sql,
            "rolling_sum.yml": rolling_sum_yml
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