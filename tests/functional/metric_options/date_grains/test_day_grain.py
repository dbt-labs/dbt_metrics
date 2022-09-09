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

# models/day_grain_metric.sql
day_grain_metric_sql = """
select *
from 
{{ dbt_metrics.calculate(metric('day_grain_metric'), 
    grain='day'
    )
}}
"""

# models/day_grain_metric.yml
day_grain_metric_yml = """
version: 2 
models:
  - name: day_grain_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('day_grain__expected')
metrics:
  - name: day_grain_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: count
    sql: order_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/day_grain__expected.csv
day_grain__expected_csv = """
date_day,day_grain_metric
2022-02-15,1
2022-02-14,0
2022-02-13,1
2022-02-12,0
2022-02-11,0
2022-02-10,0
2022-02-09,0
2022-02-08,0
2022-02-07,0
2022-02-06,0
2022-02-05,0
2022-02-04,0
2022-02-03,1
2022-02-02,0
2022-02-01,0
2022-01-31,0
2022-01-30,0
2022-01-29,0
2022-01-28,1
2022-01-27,0
2022-01-26,0
2022-01-25,0
2022-01-24,0
2022-01-23,0
2022-01-22,1
2022-01-21,1
2022-01-20,1
2022-01-19,0
2022-01-18,0
2022-01-17,0
2022-01-16,0
2022-01-15,0
2022-01-14,0
2022-01-13,1
2022-01-12,0
2022-01-11,0
2022-01-10,0
2022-01-09,0
2022-01-08,1
2022-01-07,0
2022-01-06,1
""".lstrip()

class TestDayGrain:

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
            "day_grain__expected.csv": day_grain__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "day_grain_metric.sql": day_grain_metric_sql,
            "day_grain_metric.yml": day_grain_metric_yml
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