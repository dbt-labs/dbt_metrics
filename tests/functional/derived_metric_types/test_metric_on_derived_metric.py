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

# models/metric_on_derived_metric.sql
metric_on_derived_metric_sql = """
select *
from 
{{ metrics.calculate(metric('metric_on_derived_metric'), 
    grain='month'
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
    time_grains: [day, week, month, all_time]
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

# models/derived_metric.yml
metric_on_derived_metric_yml = """
version: 2 
models:
  - name: metric_on_derived_metric
    tests: 
      - metrics.metric_equality:
          compare_model: ref('metric_on_derived_metric__expected')
metrics:
  - name: derived_metric
    label: derived ($)
    timestamp: order_date
    time_grains: [day, week, month, all_time]
    calculation_method: derived
    expression: "{{metric('base_sum_metric')}} + 1"
    dimensions:
      - had_discount
      - order_country

  - name: metric_on_derived_metric
    label: derived ($)
    timestamp: order_date
    time_grains: [day, week, month, all_time]
    calculation_method: derived
    expression: "{{metric('derived_metric')}} + 1"
    dimensions:
      - had_discount
      - order_country
"""

# seeds/metric_on_derived_metric__expected.csv
metric_on_derived_metric__expected_csv = """
date_month,base_sum_metric,derived_metric,metric_on_derived_metric
2022-02-01,6,7,8
2022-01-01,8,9,10
""".lstrip()

class TestMetricOnDerivedMetric:

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
            "metric_on_derived_metric__expected.csv": metric_on_derived_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "base_sum_metric.yml": base_sum_metric_yml,
            "metric_on_derived_metric.yml": metric_on_derived_metric_yml,
            "fact_orders.sql": fact_orders_sql,
            "metric_on_derived_metric.sql": metric_on_derived_metric_sql
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


# models/metric_on_derived_metric.sql
all_time_dimension_metric_sql = """
select *
from 
{{ metrics.calculate(metric('metric_on_derived_metric'), 
    grain='all_time',
    dimensions=['had_discount']
    )
}}
"""

# seeds/metric_on_expression_metric__expected.csv
all_time_dimension_metric__expected_csv = """
metric_start_date,metric_end_date,had_discount,base_sum_metric,metric_on_derived_metric,derived_metric
2022-01-06,2022-02-15,true,6,8,7
2022-01-08,2022-02-13,false,8,10,9
""".lstrip()

class TestAllTimeWithDimension:

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
            "metric_on_derived_metric__expected.csv": all_time_dimension_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "base_sum_metric.yml": base_sum_metric_yml,
            "metric_on_derived_metric.yml": metric_on_derived_metric_yml,
            "fact_orders.sql": fact_orders_sql,
            "metric_on_derived_metric.sql": all_time_dimension_metric_sql
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