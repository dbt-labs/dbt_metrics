from struct import pack
import os
import pytest
from dbt.tests.util import run_dbt

# our file contents
from tests.functional.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml,
    fact_orders_duplicate_source_csv,
    fact_orders_duplicate_sql,
    fact_orders_duplicate_yml,
)

# models/treat_null_values_as_zero.sql
treat_null_values_as_zero_sql = """
select *
from 
{{ metrics.calculate(
    [metric('treat_null_values_as_zero'),metric('treat_null_values_as_zero_duplicate')], 
    grain='day'
    )
}}
"""

# models/treat_null_values_as_zero.yml
treat_null_values_as_zero_yml = """
version: 2 

models:
  - name: treat_null_values_as_zero
    tests: 
      - metrics.metric_equality:
          compare_model: ref('treat_null_values_as_zero__expected')

metrics:
  - name: treat_null_values_as_zero
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
      treat_null_values_as_zero: true

  - name: treat_null_values_as_zero_duplicate
    model: ref('fact_orders_duplicate')
    label: Total Amount (Nulls)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country
    config:
      treat_null_values_as_zero: true
"""

# seeds/treat_null_values_as_zero__expected.csv
treat_null_values_as_zero__expected_csv = """
date_day,treat_null_values_as_zero,treat_null_values_as_zero_duplicate
2022-02-16,0,4
2022-02-15,4,0
2022-02-14,0,1
2022-02-13,1,0
2022-02-04,0,1
2022-02-03,1,0
2022-01-29,0,2
2022-01-28,2,0
2022-01-23,0,1
2022-01-22,1,1
2022-01-21,1,1
2022-01-20,1,0
2022-01-14,0,1
2022-01-13,1,0
2022-01-09,0,1
2022-01-08,1,0
2022-01-07,0,1
2022-01-06,1,0
""".lstrip()

class TestDefaultValueNullMetric:

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
            "fact_orders_duplicate_source.csv": fact_orders_duplicate_source_csv,
            "treat_null_values_as_zero__expected.csv": treat_null_values_as_zero__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "fact_orders.sql": fact_orders_sql,
            "fact_orders_duplicate.yml": fact_orders_duplicate_yml,
            "fact_orders_duplicate.sql": fact_orders_duplicate_sql,
            "treat_null_values_as_zero.yml": treat_null_values_as_zero_yml,
            "treat_null_values_as_zero.sql": treat_null_values_as_zero_sql
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 3

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 4

        # test tests
        results = run_dbt(["test"]) # expect passing test
        assert len(results) == 1

        # # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]

# models/treat_null_values_as_null.sql
treat_null_values_as_null_sql = """
select *
from 
{{ metrics.calculate(
    [metric('treat_null_values_as_null'),metric('treat_null_values_as_null_duplicate')], 
    grain='day'
    )
}}
"""

# models/treat_null_values_as_null.yml
treat_null_values_as_null_yml = """
version: 2 

models:
  - name: treat_null_values_as_null
    tests: 
      - metrics.metric_equality:
          compare_model: ref('treat_null_values_as_null__expected')

metrics:
  - name: treat_null_values_as_null
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
      treat_null_values_as_zero: false

  - name: treat_null_values_as_null_duplicate
    model: ref('fact_orders_duplicate')
    label: Total Amount (Nulls)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country
    config:
      treat_null_values_as_zero: false
"""

# seeds/treat_null_values_as_null__expected.csv
treat_null_values_as_null__expected_csv = """
date_day,treat_null_values_as_null,treat_null_values_as_null_duplicate
2022-02-16,,4
2022-02-15,4,
2022-02-14,,1
2022-02-13,1,
2022-02-04,,1
2022-02-03,1,
2022-01-29,,2
2022-01-28,2,
2022-01-23,,1
2022-01-22,1,1
2022-01-21,1,1
2022-01-20,1,
2022-01-14,,1
2022-01-13,1,
2022-01-09,,1
2022-01-08,1,
2022-01-07,,1
2022-01-06,1,
""".lstrip()

class TestFalseValueNullMetric:

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
            "fact_orders_duplicate_source.csv": fact_orders_duplicate_source_csv,
            "treat_null_values_as_null__expected.csv": treat_null_values_as_null__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "fact_orders.sql": fact_orders_sql,
            "fact_orders_duplicate.yml": fact_orders_duplicate_yml,
            "fact_orders_duplicate.sql": fact_orders_duplicate_sql,
            "treat_null_values_as_null.yml": treat_null_values_as_null_yml,
            "treat_null_values_as_null.sql": treat_null_values_as_null_sql
        }

    def test_build_completion(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 3

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 4

        # test tests
        results = run_dbt(["test"]) # expect passing test
        assert len(results) == 1

        # # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]
