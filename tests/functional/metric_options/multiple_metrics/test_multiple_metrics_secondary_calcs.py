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

# models/multiple_metrics.sql
multiple_metrics_sql = """
select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric'), metric('base_count_metric')],
    grain='month',
    secondary_calculations=[
    metrics.period_over_period(
        comparison_strategy="difference"
        ,interval=1
        ,metric_list=['base_sum_metric']
        ),
    metrics.period_to_date(
        aggregate="sum"
        ,period="year"
        ,metric_list=['base_sum_metric','base_count_metric']
        ),
    metrics.rolling(
        aggregate="max"
        ,interval=4
        ,metric_list='base_sum_metric'
        )
        ] 
    )
}}
"""

# models/multiple_metrics.yml
multiple_metrics_yml = """
version: 2 
models:
  - name: multiple_metrics
    tests: 
      - metrics.metric_equality:
          compare_model: ref('multiple_metrics__expected')
metrics:
  - name: base_count_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: count
    sql: order_total
    dimensions:
      - had_discount
      - order_country
  - name: base_sum_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: sum
    sql: order_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/multiple_metrics__expected.csv
multiple_metrics__expected_csv = """
date_month,date_year,base_sum_metric,base_count_metric,base_sum_metric_difference_to_1_month_ago,base_sum_metric_sum_for_year,base_count_metric_sum_for_year,base_sum_metric_rolling_max_4_month
2022-01-01,2022-01-01,8,7,8,8,7,8
2022-02-01,2022-01-01,6,3,-2,14,10,8
""".lstrip()

class TestMultipleMetricsSecondaryCalcs:

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
            "multiple_metrics__expected.csv": multiple_metrics__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "multiple_metrics.sql": multiple_metrics_sql,
            "multiple_metrics.yml": multiple_metrics_yml
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