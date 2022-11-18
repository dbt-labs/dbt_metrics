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

# models/rolling_count.sql
rolling_count_sql = """
select *
from 
{{ metrics.calculate(metric('rolling_count'), 
    grain='week',
    secondary_calculations=[
        metrics.rolling(aggregate="min"),
        metrics.rolling(aggregate="max"),
        metrics.rolling(aggregate="sum"),
        metrics.rolling(aggregate="average")
    ]
    )
}}
"""

# models/rolling_count.yml
rolling_count_yml = """
version: 2 
models:
  - name: rolling_count
    tests: 
      - metrics.metric_equality:
          compare_model: ref('rolling_count__expected')
metrics:
  - name: rolling_count
    model: ref('fact_orders')
    label: Count Distinct
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/rolling_count__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    rolling_count__expected_csv = """
date_week,rolling_count,rolling_count_rolling_min,rolling_count_rolling_max,rolling_count_rolling_sum,rolling_count_rolling_average
2022-01-03,2,2,2,2,2.000
2022-01-10,1,1,2,3,1.500
2022-01-17,3,1,3,6,2.000
2022-01-24,1,1,3,7,1.750
2022-01-31,1,1,3,8,1.600
2022-02-07,1,1,3,9,1.500
2022-02-14,1,1,3,10,1.428
""".lstrip()
else:
    rolling_count__expected_csv = """
date_week,rolling_count,rolling_count_rolling_min,rolling_count_rolling_max,rolling_count_rolling_sum,rolling_count_rolling_average
2022-01-03,2,2,2,2,2.0000000000000000
2022-01-10,1,1,2,3,1.5000000000000000
2022-01-17,3,1,3,6,2.0000000000000000
2022-01-24,1,1,3,7,1.7500000000000000
2022-01-31,1,1,3,8,1.6000000000000000
2022-02-07,1,1,3,9,1.5000000000000000
2022-02-14,1,1,3,10,1.4285714285714286
""".lstrip()

# seeds/rolling_count__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    rolling_count__expected_yml = """
version: 2
seeds:
  - name: rolling_count__expected
    config:
      column_types:
        date_week: date
        rolling_count: INT64
        rolling_count_rolling_min: INT64
        rolling_count_rolling_max: INT64
        rolling_count_rolling_sum: INT64
        rolling_count_rolling_average: FLOAT64
""".lstrip()
else: 
    rolling_count__expected_yml = """"""

class TestRollingCount:

    # configuration in dbt_project.yml
    # setting bigquery as table to get around query complexity 
    # resource constraints with compunding views
    if os.getenv('dbt_target') == 'bigquery':
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
            "rolling_count__expected.csv": rolling_count__expected_csv,
            "rolling_count__expected.yml": rolling_count__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "custom_calendar.sql": custom_calendar_sql,
            "rolling_count.sql": rolling_count_sql,
            "rolling_count.yml": rolling_count_yml
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 2

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 4

        # test tests
        results = run_dbt(["test"]) # expect passing test
        assert len(results) == 1

        # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]