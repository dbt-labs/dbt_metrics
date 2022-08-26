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

# models/base_lookback_metric.sql
base_lookback_metric_sql = """
select *
from 
{{ metrics.calculate(metric('base_lookback_metric'), 
    grain='month'
    )
}}
"""

# models/base_lookback_metric.yml
base_lookback_metric_yml = """
version: 2 
models:
  - name: base_lookback_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('base_lookback_metric__expected')
metrics:
  - name: base_lookback_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: sum
    sql: discount_total
    dimensions:
      - had_discount
      - order_country

    meta: {
      lookback: 1 year
    }
"""

# seeds/base_lookback_metric__expected.csv
base_lookback_metric__expected_csv = """
date_month,base_lookback_metric
2022-02-01,7
2022-03-01,11
2022-04-01,11
2022-05-01,11
2022-06-01,11
2022-07-01,11
2022-08-01,11
2022-09-01,11
2022-10-01,11
2022-11-01,11
2023-01-01,11
2022-12-01,11
2023-02-01,4
""".lstrip()

class TestBaseLookbackYearMetric:

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
            "base_lookback_metric__expected.csv": base_lookback_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "base_lookback_metric.sql": base_lookback_metric_sql,
            "base_lookback_metric.yml": base_lookback_metric_yml
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