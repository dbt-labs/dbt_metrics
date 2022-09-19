from struct import pack
import os
import pytest
from dbt.tests.util import run_dbt
from dbt.exceptions import CompilationException, ParsingException

# our file contents
from tests.functional.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml
)

# models/base_window_metric.yml
day_window_metric_yml = """
version: 2 

metrics:
  - name: base_window_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: discount_total
    window: 
        count: 14
        period: days
    dimensions:
      - had_discount
      - order_country
"""

class TestPluralDaysWindow:

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {"+materialized": "table"},
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
            "day_window_metric.yml": day_window_metric_yml,
        }

    def test_failing_plural_days(self,project,):
        with pytest.raises(ParsingException):
            run_dbt(["deps"])
            run_dbt(["seed"])
            run_dbt(["compile"])

week_window_metric_yml = """
version: 2 

metrics:
  - name: base_window_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: discount_total
    window: 
        count: 14
        period: weeks
    dimensions:
      - had_discount
      - order_country
"""

class TestPluralWeeksWindow:

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {"+materialized": "table"},
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
            "week_window_metric.yml": week_window_metric_yml,
        }

    def test_failing_plural_weeks(self,project,):
        with pytest.raises(ParsingException):
            run_dbt(["deps"])
            run_dbt(["seed"])
            run_dbt(["compile"])

month_window_metric_yml = """
version: 2 

metrics:
  - name: base_window_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: discount_total
    window: 
        count: 14
        period: months
    dimensions:
      - had_discount
      - order_country
"""

class TestPluralMonthWindow:

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {"+materialized": "table"},
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
            "month_window_metric.yml": month_window_metric_yml,
        }

    def test_failing_plural_months(self,project,):
        with pytest.raises(ParsingException):
            run_dbt(["deps"])
            run_dbt(["seed"])
            run_dbt(["compile"])

year_window_metric_yml = """
version: 2 

metrics:
  - name: base_window_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: discount_total
    window: 
        count: 14
        period: years
    dimensions:
      - had_discount
      - order_country
"""

class TestPluralYearsWindow:

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {"+materialized": "table"},
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
            "year_window_metric.yml": year_window_metric_yml,
        }

    def test_failing_plural_years(self,project,):
        with pytest.raises(ParsingException):
            run_dbt(["deps"])
            run_dbt(["seed"])
            run_dbt(["compile"])