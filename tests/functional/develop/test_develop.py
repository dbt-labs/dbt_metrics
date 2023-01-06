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

# models/develop_metric_monthly.sql
develop_metric_monthly_sql = """
{% set my_metric_yml -%}

metrics:
  - name: develop_metric_monthly
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: discount_total
    dimensions:
      - had_discount
      - order_country
    config:
      treat_null_values_as_zero: false

{%- endset %}

select * 
from {{ metrics.develop(
        develop_yml=my_metric_yml,
        metric_list=['develop_metric_monthly'],
        grain='month'
        )
    }}
"""

# models/develop_metric_monthly.yml
develop_metric_monthly_yml = """
version: 2 
models:
  - name: develop_metric_monthly
    tests: 
      - metrics.metric_equality:
          compare_model: ref('develop_metric_monthly__expected')

"""

# seeds/develop_metric__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    develop_metric_monthly__expected_csv = """
date_month,develop_metric_monthly
2022-01-01,1.000000
2022-02-01,1.333333
""".lstrip()
else:
    develop_metric_monthly__expected_csv = """
date_month,develop_metric_monthly
2022-01-01,1.00000000000000000000
2022-02-01,1.3333333333333333
""".lstrip()

# seeds/develop_metric_monthly___expected.yml
if os.getenv('dbt_target') == 'bigquery':
    develop_metric_monthly__expected_yml = """
version: 2
seeds:
  - name: develop_metric__expected
    config:
      column_types:
        date_month: date
        develop_metric_monthly: FLOAT64
""".lstrip()
else: 
    develop_metric_monthly__expected_yml = """"""

class TestDevelopMonthlyMetric:
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
            "develop_metric_monthly__expected.csv": develop_metric_monthly__expected_csv,
            "develop_metric_monthly__expected.yml": develop_metric_monthly__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "develop_metric_monthly.sql": develop_metric_monthly_sql,
            "develop_metric_monthly.yml": develop_metric_monthly_yml
        }

    def test_develop_monthly_metric(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 2

        # initial run
        results = run_dbt(["run"])
        # breakpoint()
        assert len(results) == 3

        # test tests
        results = run_dbt(["test"]) # expect passing test
        assert len(results) == 1

        # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]

# models/develop_metric.sql
develop_multiple_metrics_sql = """
{% set my_metric_yml -%}
{% raw %}

metrics:
  - name: develop_metric_multiple
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: discount_total
    dimensions:
      - had_discount
      - order_country

  - name: derived_metric_multiple
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{ metric('develop_metric_multiple') }} - 1 "
    dimensions:
      - had_discount
      - order_country

  - name: some_other_metric_not_using_multiple
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{ metric('derived_metric_multiple') }} - 1 "
    dimensions:
      - had_discount
      - order_country

{% endraw %}
{%- endset %}

select * 
from {{ metrics.develop(
        develop_yml=my_metric_yml,
        metric_list=['derived_metric_multiple'],
        grain='month'
        )
    }}
"""

# models/develop_multiple_metrics.yml
develop_multiple_metrics_yml = """
version: 2 
models:
  - name: develop_multiple_metrics
    tests: 
      - metrics.metric_equality:
          compare_model: ref('develop_multiple_metrics__expected')

"""

# seeds/develop_multiple_metrics___expected.yml
if os.getenv('dbt_target') == 'snowflake':
    develop_multiple_metrics__expected_csv = """
date_month,develop_metric_multiple,derived_metric_multiple
2022-02-01,1.333333,0.333333
2022-01-01,1.0,0.0
""".lstrip()
else: 
    develop_multiple_metrics__expected_csv = """
date_month,develop_metric_multiple,derived_metric_multiple
2022-02-01,1.3333333333333333,0.33333333333333326
2022-01-01,1.0,0.0
""".lstrip()

# seeds/develop_multiple_metrics___expected.yml
if os.getenv('dbt_target') == 'bigquery':
    develop_multiple_metrics__expected_yml = """
version: 2
seeds:
  - name: develop_multiple_metrics__expected
    config:
      column_types:
        date_month: date
        develop_metric_multiple: FLOAT64
        derived_metric_multiple: FLOAT64
""".lstrip()
else: 
    develop_multiple_metrics__expected_yml = """"""

class TestDevelopMultipleMetrics:
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
            "develop_multiple_metrics__expected.csv": develop_multiple_metrics__expected_csv,
            "develop_multiple_metrics__expected.yml": develop_multiple_metrics__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "develop_multiple_metrics.sql": develop_multiple_metrics_sql,
            "develop_multiple_metrics.yml": develop_multiple_metrics_yml
        }

    def test_develop_multiple_metrics(self,project,):
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


# models/develop_metric_window.sql
develop_metric_window_sql = """
{% set my_metric_yml -%}
metrics:
  - name: develop_metric_window
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: discount_total
    window: 
        count: 14 
        period: day
    dimensions:
      - had_discount
      - order_country
{%- endset %}
select * 
from {{ metrics.develop(
        develop_yml=my_metric_yml,
        metric_list=['develop_metric_window'],
        grain='week'
        )
    }}
"""

# models/develop_metric_window.yml
develop_metric_window_yml = """
version: 2 
models:
  - name: develop_metric_window
    tests: 
      - metrics.metric_equality:
          compare_model: ref('develop_metric_window__expected')
"""

# seeds/develop_metric__expected.csv
develop_metric_window__expected_csv = """
date_week,develop_metric_window
2022-01-10,2
2022-01-17,3
2022-01-24,4
2022-01-31,4
2022-02-07,2
2022-02-14,2
2022-02-21,3
2022-02-28,2
""".lstrip()

class TestDevelopMetricWindow:
    # configuration in dbt_project.yml
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
            "develop_metric_window__expected.csv": develop_metric_window__expected_csv
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "develop_metric_window.sql": develop_metric_window_sql,
            "develop_metric_window.yml": develop_metric_window_yml,
            "custom_calendar.sql": custom_calendar_sql
        }

    def test_develop_metric_window(self,project,):
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


# models/develop_metric_dimension.sql
develop_metric_dimension_sql = """
{% set my_metric_yml -%}

metrics:
  - name: develop_metric_dimension
    model: ref('fact_orders')
    label: develop metric dimensions
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country

{%- endset %}

select * 
from {{ metrics.develop(
        develop_yml=my_metric_yml,
        metric_list='develop_metric_dimension',
        grain='month',
        dimensions=['had_discount','order_country']
        )
    }}
"""

# models/develop_metric_dimension.yml
develop_metric_dimension_yml = """
version: 2 
models:
  - name: develop_metric_dimension
    tests: 
      - metrics.metric_equality:
          compare_model: ref('develop_metric_dimension__expected')

"""

# seeds/develop_metric_dimension__expected.csv
develop_metric_dimension__expected_csv = """
date_month,had_discount,order_country,develop_metric_dimension
2022-01-01,TRUE,France,1
2022-01-01,TRUE,Japan,1
2022-01-01,FALSE,France,4
2022-01-01,FALSE,Japan,2
2022-02-01,TRUE,France,4
2022-02-01,FALSE,Japan,2
""".lstrip()

class TestDevelopMetricDimension:
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
            "develop_metric_dimension__expected.csv": develop_metric_dimension__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "develop_metric_dimension.sql": develop_metric_dimension_sql,
            "develop_metric_dimension.yml": develop_metric_dimension_yml
        }

    def test_develop_metric_dimension(self,project,):
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


# models/testing_metric__develop.sql
testing_metric_develop_sql = """
{% set my_metric_yml -%}

metrics:
  - name: testing_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: discount_total
    dimensions:
      - had_discount
      - order_country

{%- endset %}

select * 
from {{ metrics.develop(
        develop_yml=my_metric_yml,
        metric_list='testing_metric',
        grain='month'
        )
    }}
"""

# models/testing_metric_calculate.yml
testing_metric_calculate_yml = """
version: 2 

metrics:
  - name: testing_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: discount_total
    dimensions:
      - had_discount
      - order_country
"""

# models/testing_metric_calculate.sql
testing_metric_calculate_sql = """
select *
from 
{{ metrics.calculate(metric('testing_metric'), 
    grain='month'
    )
}}
"""

# models/testing_metric_develop.yml
testing_metric_develop_yml = """
version: 2 
models:
  - name: testing_metric_develop
    tests: 
      - metrics.metric_equality:
          compare_model: ref('testing_metric_calculate')

"""

class TestDevelopMatchesCalculate:
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
            "fact_orders_source.csv": fact_orders_source_csv
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "testing_metric_develop.sql": testing_metric_develop_sql,
            "testing_metric_develop.yml": testing_metric_develop_yml,
            "testing_metric_calculate.sql": testing_metric_calculate_sql,
            "testing_metric_calculate.yml": testing_metric_calculate_yml
        }

    def test_develop_matches_calculate_second_run(self,project,):
        # running deps to install package
        results = run_dbt(["deps"])

        # seed seeds
        results = run_dbt(["seed"])
        assert len(results) == 1

        # initial run
        results = run_dbt(["run"])
        assert len(results) == 4

        # test tests
        results = run_dbt(["test"]) # expect passing test
        assert len(results) == 1

        # # # validate that the results include pass
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["pass"]