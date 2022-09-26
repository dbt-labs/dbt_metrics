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

# models/multi_dimension_derived_metric.sql
multi_dimension_derived_metric_sql = """
select *
from 
{{ metrics.calculate(metric('multi_dimension_derived_metric'), 
    grain='month',
    dimensions=['had_discount','order_country']
    )
}}
"""

# models/base_sum_metric.yml
base_sum_metric_yml = """
version: 2 
metrics:
  - name: base_sum_metric
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

# models/multi_dimension_derived_metric.yml
multi_dimension_derived_metric_yml = """
version: 2 
models:
  - name: multi_dimension_derived_metric
    tests: 
      - dbt_utils.equality:
          compare_model: ref('multi_dimension_derived_metric__expected')
metrics:
  - name: multi_dimension_derived_metric
    label: derived ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{metric('base_sum_metric')}} + 1"
    dimensions:
      - had_discount
      - order_country
"""

# seeds/multi_dimension_derived_metric__expected.csv
multi_dimension_derived_metric__expected_csv = """
date_month,had_discount,order_country,base_sum_metric,multi_dimension_derived_metric
2022-01-01,TRUE,France,1,2
2022-01-01,TRUE,Japan,1,2
2022-01-01,FALSE,France,4,5
2022-01-01,FALSE,Japan,2,3
2022-02-01,TRUE,France,4,5
2022-02-01,FALSE,France,0,1
2022-02-01,FALSE,Japan,2,3
2022-02-01,TRUE,Japan,0,1
""".lstrip()

class TestMultiDimensionDerivedMetric:

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
            "multi_dimension_derived_metric__expected.csv": multi_dimension_derived_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "base_sum_metric.yml": base_sum_metric_yml,
            "multi_dimension_derived_metric.yml": multi_dimension_derived_metric_yml,
            "fact_orders.sql": fact_orders_sql,
            "multi_dimension_derived_metric.sql": multi_dimension_derived_metric_sql
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

        # # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]