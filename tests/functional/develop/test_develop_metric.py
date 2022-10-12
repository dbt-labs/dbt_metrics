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

# models/develop_metric.sql
develop_metric_sql = """
{% set my_metric_yml -%}

metrics:
  - name: develop_metric
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
        metric_list='develop_metric',
        grain='month'
        )
    }}
"""

# models/develop_metric.yml
develop_metric_yml = """
version: 2 
models:
  - name: develop_metric
    tests: 
      - metrics.metric_equality:
          compare_model: ref('develop_metric__expected')

"""

# seeds/develop_metric__expected.csv
if os.getenv('dbt_target') == 'postgres':
    develop_metric__expected_csv = """
date_month,develop_metric
2022-01-01,1.00000000000000000000
2022-02-01,1.3333333333333333
""".lstrip()

# seeds/develop_metric__expected.csv
if os.getenv('dbt_target') == 'redshift':
    develop_metric__expected_csv = """
date_month,develop_metric
2022-01-01,1.00000000000000000000
2022-02-01,1.3333333333333333
""".lstrip()

# seeds/develop_metric__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    develop_metric__expected_csv = """
date_month,develop_metric
2022-01-01,1.000000
2022-02-01,1.333333
""".lstrip()

# seeds/develop_metric__expected.csv
if os.getenv('dbt_target') == 'bigquery':
    develop_metric__expected_csv = """
date_month,develop_metric
2022-01-01,1.00000000000000000000
2022-02-01,1.3333333333333333
""".lstrip()

# seeds/develop_metric___expected.yml
if os.getenv('dbt_target') == 'bigquery':
    develop_metric__expected_yml = """
version: 2
seeds:
  - name: develop_metric__expected
    config:
      column_types:
        date_month: date
        develop_metric: FLOAT64
""".lstrip()
else: 
    develop_metric__expected_yml = """"""

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
            "develop_metric__expected.csv": develop_metric__expected_csv,
            "develop_metric__expected.yml": develop_metric__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "develop_metric.sql": develop_metric_sql,
            "develop_metric.yml": develop_metric_yml
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

# models/develop_metric.sql
develop_multiple_metrics_sql = """
{% set my_metric_yml -%}
{% raw %}

metrics:
  - name: develop_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: discount_total
    dimensions:
      - had_discount
      - order_country

  - name: derived_metric
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{ metric('develop_metric') }} - 1 "
    dimensions:
      - had_discount
      - order_country

  - name: some_other_metric_not_using
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{ metric('derived_metric') }} - 1 "
    dimensions:
      - had_discount
      - order_country

{% endraw %}
{%- endset %}

select * 
from {{ metrics.develop(
        develop_yml=my_metric_yml,
        metric_list=['derived_metric'],
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
date_month,develop_metric,derived_metric
2022-02-01,1.333333,0.333333
2022-01-01,1.0,0.0
""".lstrip()
else: 
    develop_multiple_metrics__expected_csv = """
date_month,develop_metric,derived_metric
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
        develop_metric: FLOAT64
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