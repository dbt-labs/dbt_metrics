from struct import pack
import os
import pytest
from dbt.tests.util import run_dbt

# our file contents
from tests.functional.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml
)

# models/base_median_metric.sql
base_median_metric_sql = """
select *
from 
{{ metrics.calculate(metric('base_median_metric'), 
    grain='month'
    )
}}
"""

# models/base_median_metric.yml
base_median_metric_yml = """
version: 2 
models:
  - name: base_median_metric
    tests: 
      - metrics.metric_equality:
          compare_model: ref('base_median_metric__expected')
metrics:
  - name: base_median_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: median
    expression: discount_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/base_median_metric__expected.csv
base_median_metric__expected_csv = """
date_month,base_median_metric
2022-01-01,1
2022-02-01,1
""".lstrip()

class TestBaseMedianMetric:
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
            "base_median_metric__expected.csv": base_median_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "base_median_metric.sql": base_median_metric_sql,
            "base_median_metric.yml": base_median_metric_yml
        }

    def test_base_median_metric(self,project,):
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

# models/base_median_metric_no_time_grain.sql
base_median_metric_no_time_grain_sql = """
select *
from 
{{ metrics.calculate(metric('base_median_metric_no_time_grain'))
}}
"""

# models/base_median_metric_no_time_grain.yml
base_median_metric_no_time_grain_yml = """
version: 2 
models:
  - name: base_median_metric_no_time_grain
    tests: 
      - metrics.metric_equality:
          compare_model: ref('base_median_metric_no_time_grain__expected')
metrics:
  - name: base_median_metric_no_time_grain
    model: ref('fact_orders')
    label: Total Discount ($)
    calculation_method: median
    expression: discount_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/base_median_metric_no_time_grain__expected.csv
base_median_metric_no_time_grain__expected_csv = """
base_median_metric_no_time_grain
1
""".lstrip()

class TestBaseMedianMetricNoTimeGrain:
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
            "base_median_metric_no_time_grain__expected.csv": base_median_metric_no_time_grain__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "base_median_metric_no_time_grain.sql": base_median_metric_no_time_grain_sql,
            "base_median_metric_no_time_grain.yml": base_median_metric_no_time_grain_yml
        }

    def test_base_median_metric_no_time_grain(self,project,):
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


# seeds/complicated_median_source.csv
complicated_median_source_csv = """
order_id,order_country,had_discount,customer_id,order_date,order_total
4,France,true,3,2022-01-06,1.43
5,France,false,4,2022-01-08,4.29
3,France,false,1,2022-01-13,6.56
2,Japan,false,2,2022-01-20,5.93
6,Japan,false,5,2022-01-21,1.01
7,Japan,true,2,2022-01-22,2.7
1,France,false,1,2022-01-28,3.34
9,Japan,false,2,2022-02-03,7.11
10,Japan,false,3,2022-02-13,5.89
8,France,true,1,2022-02-15,9.12
""".lstrip()

# models/base_median_metric_complicated_source.sql
base_median_metric_complicated_source_sql = """
select *
from 
{{ metrics.calculate(metric('base_median_metric_complicated_source'),
    grain='month'
    )
}}
"""

# models/base_median_metric_complicated_source.yml
base_median_metric_complicated_source_yml = """
version: 2 
models:
  - name: base_median_metric_complicated_source
    tests: 
      - metrics.metric_equality:
          compare_model: ref('base_median_metric_complicated_source__expected')
metrics:
  - name: base_median_metric_complicated_source
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: median
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/base_median_metric_complicated_source_expected.csv
base_median_metric_complicated_source__expected_csv = """
date_month,base_median_metric_complicated_source
2022-01-01,3.34
2022-02-01,7.11
""".lstrip()

class TestBaseMedianMetricComplicatedSource:
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
            "fact_orders_source.csv": complicated_median_source_csv,
            "base_median_metric_complicated_source__expected.csv": base_median_metric_complicated_source__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "base_median_metric_complicated_source.sql": base_median_metric_complicated_source_sql,
            "base_median_metric_complicated_source.yml": base_median_metric_complicated_source_yml
        }

    def test_base_median_metric_complicated_source(self,project,):
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