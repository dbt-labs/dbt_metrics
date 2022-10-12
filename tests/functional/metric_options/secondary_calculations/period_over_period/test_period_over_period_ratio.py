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

# models/period_over_period_ratio.sql
period_over_period_ratio_sql = """
select *
from 
{{ metrics.calculate(metric('period_over_period_ratio'), 
    grain='month',
    secondary_calculations=[
        metrics.period_over_period(comparison_strategy="ratio", interval=1, alias = "1mth")
    ]
    )
}}
"""

# models/period_over_period_ratio.yml
period_over_period_ratio_yml = """
version: 2 
models:
  - name: period_over_period_ratio
    tests: 
      - metrics.metric_equality:
          compare_model: ref('period_over_period_ratio__expected')
metrics:
  - name: period_over_period_ratio
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

# seeds/period_over_period_ratio__expected.csv
period_over_period_ratio__expected_csv = """
date_month,period_over_period_ratio,period_over_period_ratio_1mth
2022-01-01,8,0
2022-02-01,6,0.75
""".lstrip()

# seeds/period_over_period_ratio___expected.yml
if os.getenv('dbt_target') == 'bigquery':
    period_over_period_ratio__expected_yml = """
version: 2
seeds:
  - name: period_over_period_ratio__expected
    config:
      column_types:
        date_month: date
        period_over_period_ratio: INT64
        period_over_period_ratio_1mth: FLOAT64
""".lstrip()
else: 
    period_over_period_ratio__expected_yml = """"""

class TestPeriodOverPeriodRatio:

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
            "period_over_period_ratio__expected.csv": period_over_period_ratio__expected_csv,
            "period_over_period_ratio__expected.yml": period_over_period_ratio__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "period_over_period_ratio.sql": period_over_period_ratio_sql,
            "period_over_period_ratio.yml": period_over_period_ratio_yml
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