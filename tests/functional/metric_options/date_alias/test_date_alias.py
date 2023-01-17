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

# models/date_alias_single_quote_base_metric.sql
date_alias_single_quote_base_metric_sql = """
select *
from 
{{ metrics.calculate(metric('date_alias_single_quote_base_metric'), 
    grain='month',
    dimensions=['had_discount'],
    date_alias='date'
    )
}}
"""

# models/date_alias_single_quote_base_metric.yml
date_alias_single_quote_base_metric_yml = """
version: 2 
models:
  - name: date_alias_single_quote_base_metric
    tests: 
      - metrics.metric_equality:
          compare_model: ref('date_alias_single_quote_base_metric__expected')
metrics:
  - name: date_alias_single_quote_base_metric
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

# seeds/date_alias_single_quote_base_metric__expected.csv
date_alias_single_quote_base_metric__expected_csv = """
date,had_discount,date_alias_single_quote_base_metric
2022-01-01,TRUE,2
2022-01-01,FALSE,6
2022-02-01,TRUE,4
2022-02-01,FALSE,2
""".lstrip()

class TestDateAliasSingleQuoteMetric:

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
            "date_alias_single_quote_base_metric__expected.csv": date_alias_single_quote_base_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "date_alias_single_quote_base_metric.sql": date_alias_single_quote_base_metric_sql,
            "date_alias_single_quote_base_metric.yml": date_alias_single_quote_base_metric_yml
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

# models/date_alias_double_quote_base_metric.sql
date_alias_double_quote_base_metric_sql = """
select *
from 
{{ metrics.calculate(metric('date_alias_double_quote_base_metric'), 
    grain='month',
    dimensions=['had_discount'],
    date_alias="date"
    )
}}
"""

# models/date_alias_double_quote_base_metric.yml
date_alias_double_quote_base_metric_yml = """
version: 2 
models:
  - name: date_alias_double_quote_base_metric
    tests: 
      - metrics.metric_equality:
          compare_model: ref('date_alias_double_quote_base_metric__expected')
metrics:
  - name: date_alias_double_quote_base_metric
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

# seeds/date_alias_double_quote_base_metric__expected.csv
date_alias_double_quote_base_metric__expected_csv = """
date,had_discount,date_alias_double_quote_base_metric
2022-01-01,TRUE,2
2022-01-01,FALSE,6
2022-02-01,TRUE,4
2022-02-01,FALSE,2
""".lstrip()

class TestDateAliasDoubleQuoteMetric:

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
            "date_alias_double_quote_base_metric__expected.csv": date_alias_double_quote_base_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "fact_orders.sql": fact_orders_sql,
            "date_alias_double_quote_base_metric.sql": date_alias_double_quote_base_metric_sql,
            "date_alias_double_quote_base_metric.yml": date_alias_double_quote_base_metric_yml,
        }

    def test_date_alias_double_quote(self,project,):
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