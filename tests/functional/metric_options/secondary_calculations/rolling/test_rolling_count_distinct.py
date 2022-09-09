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

# models/rolling_count_distinct.sql
rolling_count_distinct_sql = """
select *
from 
{{ dbt_metrics.calculate(metric('rolling_count_distinct'), 
    grain='month',
    secondary_calculations=[
        dbt_metrics.rolling(aggregate="min", interval=2),
        dbt_metrics.rolling(aggregate="max", interval=2),
        dbt_metrics.rolling(aggregate="sum", interval=2),
        dbt_metrics.rolling(aggregate="average", interval=2)
    ]
    )
}}
"""

# models/rolling_count_distinct.yml
rolling_count_distinct_yml = """
version: 2 
models:
  - name: rolling_count_distinct
    tests: 
      - dbt_utils.equality:
          compare_model: ref('rolling_count_distinct__expected')
metrics:
  - name: rolling_count_distinct
    model: ref('fact_orders')
    label: Count Distinct
    timestamp: order_date
    time_grains: [day, week, month]
    type: count_distinct
    sql: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_count_distinct__expected.csv
if os.getenv('dbt_target') == 'postgres':
    rolling_count_distinct__expected_csv = """
date_month,rolling_count_distinct,rolling_count_distinct_rolling_min_2_month,rolling_count_distinct_rolling_max_2_month,rolling_count_distinct_rolling_sum_2_month,rolling_count_distinct_rolling_average_2_month
2022-01-01,5,5,5,5,5.0000000000000000
2022-02-01,3,3,5,8,4.0000000000000000
""".lstrip()

# seeds/rolling_count_distinct__expected.csv
if os.getenv('dbt_target') == 'redshift':
    rolling_count_distinct__expected_csv = """
date_month,rolling_count_distinct,rolling_count_distinct_rolling_min_2_month,rolling_count_distinct_rolling_max_2_month,rolling_count_distinct_rolling_sum_2_month,rolling_count_distinct_rolling_average_2_month
2022-01-01,5,5,5,5,5.0000000000000000
2022-02-01,3,3,5,8,4.0000000000000000
""".lstrip()

# seeds/rolling_count_distinct__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    rolling_count_distinct__expected_csv = """
date_month,rolling_count_distinct,rolling_count_distinct_rolling_min_2_month,rolling_count_distinct_rolling_max_2_month,rolling_count_distinct_rolling_sum_2_month,rolling_count_distinct_rolling_average_2_month
2022-01-01,5,5,5,5,5.000000
2022-02-01,3,3,5,8,4.000000
""".lstrip()

# seeds/rolling_count_distinct__expected.csv
if os.getenv('dbt_target') == 'bigquery':
    rolling_count_distinct__expected_csv = """
date_month,rolling_count_distinct,rolling_count_distinct_rolling_min_2_month,rolling_count_distinct_rolling_max_2_month,rolling_count_distinct_rolling_sum_2_month,rolling_count_distinct_rolling_average_2_month
2022-01-01,5,5,5,5,5.0000000000000000
2022-02-01,3,3,5,8,4.0000000000000000
""".lstrip()

# seeds/rolling_count_distinct__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_count_distinct__expected_yml = """
version: 2
seeds:
  - name: rolling_count_distinct__expected
    config:
      column_types:
        date_month: date
        rolling_count_distinct: INT64
        rolling_count_distinct_rolling_min_2_month: INT64
        rolling_count_distinct_rolling_max_2_month: INT64
        rolling_count_distinct_rolling_sum_2_month: INT64
        rolling_count_distinct_rolling_average_2_month: FLOAT64
""".lstrip()
else: 
    rolling_count_distinct__expected_yml = """"""

class TestRollingCountDistinct:

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
            "rolling_count_distinct__expected.csv": rolling_count_distinct__expected_csv,
            "rolling_count_distinct__expected.yml": rolling_count_distinct__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "rolling_count_distinct.sql": rolling_count_distinct_sql,
            "rolling_count_distinct.yml": rolling_count_distinct_yml
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