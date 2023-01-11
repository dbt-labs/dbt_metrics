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

# models/prior_metric.sql
prior_metric_sql = """
select *
from 
{{ metrics.calculate(metric('prior_metric'), 
    grain='week',
    secondary_calculations=[metrics.prior(interval=2)]
    )
}}
"""

# models/prior_metric.yml
prior_metric_yml = """
version: 2 
models:
  - name: prior_metric
    tests: 
      - metrics.metric_equality:
          compare_model: ref('prior_metric__expected')
metrics:
  - name: prior_metric
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

# seeds/prior_metric__expected.csv
prior_metric__expected_csv = """
date_week,prior_metric,prior_metric_2_weeks_prior
2022-02-14,1,1
2022-02-07,1,1
2022-01-31,1,3
2022-01-24,1,1
2022-01-17,3,2
2022-01-10,1,
2022-01-03,2,
""".lstrip()


# seeds/prior_metric__expected.yml
prior_metric__expected_yml = """
version: 2
seeds:
  - name: prior_metric__expected
""".lstrip()

class TestPrior:

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
            "prior_metric__expected.csv": prior_metric__expected_csv,
            "prior_metric__expected.yml": prior_metric__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "custom_calendar.sql": custom_calendar_sql,
            "prior_metric.sql": prior_metric_sql,
            "prior_metric.yml": prior_metric_yml
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