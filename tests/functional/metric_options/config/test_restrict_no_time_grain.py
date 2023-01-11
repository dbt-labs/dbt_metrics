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

# models/restrict_no_time_grain_false.sql
restrict_no_time_grain_false_sql = """
select *
from 
{{ metrics.calculate(
    [metric('restrict_no_time_grain_false')], 
    grain='day'
    )
}}
"""

# models/restrict_no_time_grain_false.yml
restrict_no_time_grain_false_yml = """
version: 2 

models:
  - name: restrict_no_time_grain_false
    tests: 
      - metrics.metric_equality:
          compare_model: ref('restrict_no_time_grain_false__expected')

metrics:
  - name: restrict_no_time_grain_false
    model: ref('fact_orders')
    label: Total Amount (Nulls)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country
    config:
      restrict_no_time_grain: false

"""

# seeds/restrict_no_time_grain_false__expected.csv
restrict_no_time_grain_false__expected_csv = """
date_day,restrict_no_time_grain_false
2022-02-15,4
2022-02-13,1
2022-02-03,1
2022-01-28,2
2022-01-22,1
2022-01-21,1
2022-01-20,1
2022-01-13,1
2022-01-08,1
2022-01-06,1
""".lstrip()

class TestRestrictNoTimeGrainFalseMetric:

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
            "restrict_no_time_grain_false__expected.csv": restrict_no_time_grain_false__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "fact_orders.sql": fact_orders_sql,
            "restrict_no_time_grain_false.yml": restrict_no_time_grain_false_yml,
            "restrict_no_time_grain_false.sql": restrict_no_time_grain_false_sql
        }

    def test_restrict_no_time_grain_false(self,project,):
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

# models/restrict_no_time_grain_true.sql
restrict_no_time_grain_true_sql = """
select *
from 
{{ metrics.calculate(
    [metric('restrict_no_time_grain_true')], 
    )
}}
"""

# models/restrict_no_time_grain_true.yml
restrict_no_time_grain_true_yml = """
version: 2 

models:
  - name: restrict_no_time_grain_true

metrics:
  - name: restrict_no_time_grain_true
    model: ref('fact_orders')
    label: Total Amount (Nulls)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country
    config:
      restrict_no_time_grain: true

"""

class TestRestrictNoTimeGrainTrueMetric:

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
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "fact_orders.sql": fact_orders_sql,
            "restrict_no_time_grain_true.yml": restrict_no_time_grain_true_yml,
            "restrict_no_time_grain_true.sql": restrict_no_time_grain_true_sql
        }

    def test_restrict_no_time_grain_true(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])
        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 1
        # initial run
        run_dbt(["run"],expect_pass = False)