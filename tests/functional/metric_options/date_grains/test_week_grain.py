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

# models/week_grain_metric.sql
week_grain_metric_sql = """
select *
from 
{{ metrics.calculate(metric('week_grain_metric'), 
    grain='week'
    )
}}
"""

# models/week_grain_metric.yml
week_grain_metric_yml = """
version: 2 
models:
  - name: week_grain_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('week_grain__expected')
metrics:
  - name: week_grain_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

if os.getenv('dbt_target') == 'bigquery':
    # seeds/week_grain__expected.csv
    week_grain__expected_csv = """
date_week,week_grain_metric
2022-02-13,2
2022-02-06,0
2022-01-30,1
2022-01-23,1
2022-01-16,3
2022-01-09,1
2022-01-02,2
""".lstrip()
else: 
    # seeds/week_grain__expected.csv
    week_grain__expected_csv = """
date_week,week_grain_metric
2022-02-14,1
2022-02-07,1
2022-01-31,1
2022-01-24,1
2022-01-17,3
2022-01-10,1
2022-01-03,2
""".lstrip()

class TestWeekGrain:

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
            "week_grain__expected.csv": week_grain__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "week_grain_metric.sql": week_grain_metric_sql,
            "week_grain_metric.yml": week_grain_metric_yml
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