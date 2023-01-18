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

# models/period_to_date_average.sql
period_to_date_average_sql = """
select *
from 
{{ metrics.calculate(metric('period_to_date_average'), 
    grain='month',
    secondary_calculations=[
        metrics.period_to_date(aggregate="min", period="year", alias="this_year_min"),
        metrics.period_to_date(aggregate="max", period="year"),
    ]
    )
}}
"""

# models/period_to_date_average.yml
period_to_date_average_yml = """
version: 2 
models:
  - name: period_to_date_average
    tests: 
      - metrics.metric_equality:
          compare_model: ref('period_to_date_average__expected')
metrics:
  - name: period_to_date_average
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

# seeds/period_to_date_average__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    period_to_date_average__expected_csv = """
date_month,date_year,period_to_date_average,period_to_date_average_this_year_min,period_to_date_average_max_for_year
2022-01-01,2022-01-01,1.000000,1,1
2022-02-01,2022-01-01,1.333333,1,1.333333
""".lstrip()
else:
    period_to_date_average__expected_csv = """
date_month,date_year,period_to_date_average,period_to_date_average_this_year_min,period_to_date_average_max_for_year
2022-01-01,2022-01-01,1.00000000000000000000,1,1
2022-02-01,2022-01-01,1.3333333333333333,1,1.3333333333333333
""".lstrip()

# seeds/period_to_date_average__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    period_to_date_average__expected_yml = """
version: 2
seeds:
  - name: period_to_date_average__expected
    config:
      column_types:
        date_month: date
        date_year: date
        period_to_date_average: FLOAT64
        period_to_date_average_this_year_min: INT64
        period_to_date_average_max_for_year: FLOAT64
""".lstrip()
else: 
    period_to_date_average__expected_yml = """"""


class TestPeriodToDateAverage:

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
            "period_to_date_average__expected.csv": period_to_date_average__expected_csv,
            "period_to_date_average__expected.yml": period_to_date_average__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "period_to_date_average.sql": period_to_date_average_sql,
            "period_to_date_average.yml": period_to_date_average_yml
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

# models/period_to_date_count_distinct.sql
period_to_date_count_distinct_sql = """
select *
from 
{{ metrics.calculate(metric('period_to_date_count_distinct'), 
    grain='month',
    secondary_calculations=[
        metrics.period_to_date(aggregate="min", period="year", alias="this_year_min"),
        metrics.period_to_date(aggregate="max", period="year"),
        metrics.period_to_date(aggregate="sum", period="year"),
        metrics.period_to_date(aggregate="average", period="year"),
    ]
    )
}}
"""

# models/period_to_date_count_distinct.yml
period_to_date_count_distinct_yml = """
version: 2 
models:
  - name: period_to_date_count_distinct
    tests: 
      - metrics.metric_equality:
          compare_model: ref('period_to_date_count_distinct__expected')
metrics:
  - name: period_to_date_count_distinct
    model: ref('fact_orders')
    label: Count Distinct
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count_distinct
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/period_to_date_count_distinct__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    period_to_date_count_distinct__expected_csv = """
date_month,date_year,period_to_date_count_distinct,period_to_date_count_distinct_this_year_min,period_to_date_count_distinct_max_for_year,period_to_date_count_distinct_sum_for_year,period_to_date_count_distinct_average_for_year
2022-01-01,2022-01-01,5,5,5,5,5.000000
2022-02-01,2022-01-01,3,3,5,8,4.000000
""".lstrip()
else:
    period_to_date_count_distinct__expected_csv = """
date_month,date_year,period_to_date_count_distinct,period_to_date_count_distinct_this_year_min,period_to_date_count_distinct_max_for_year,period_to_date_count_distinct_sum_for_year,period_to_date_count_distinct_average_for_year
2022-01-01,2022-01-01,5,5,5,5,5.0000000000000000
2022-02-01,2022-01-01,3,3,5,8,4.0000000000000000
""".lstrip()

# seeds/period_to_date_count_distinct__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    period_to_date_count_distinct__expected_yml = """
version: 2
seeds:
  - name: period_to_date_count_distinct__expected
    config:
      column_types:
        date_month: date
        date_year: date
        period_to_date_count_distinct: INT64
        period_to_date_count_distinct_this_year_min: INT64
        period_to_date_count_distinct_max_for_year: INT64
        period_to_date_count_distinct_sum_for_year: INT64
        period_to_date_count_distinct_average_for_year: FLOAT64
""".lstrip()
else: 
    period_to_date_count_distinct__expected_yml = """"""

class TestPeriodToDateCountDistinct:

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
            "period_to_date_count_distinct__expected.csv": period_to_date_count_distinct__expected_csv,
            "period_to_date_count_distinct__expected.yml": period_to_date_count_distinct__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "period_to_date_count_distinct.sql": period_to_date_count_distinct_sql,
            "period_to_date_count_distinct.yml": period_to_date_count_distinct_yml
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

# models/period_to_date_count.sql
period_to_date_count_sql = """
select *
from 
{{ metrics.calculate(metric('period_to_date_count'), 
    grain='month',
    secondary_calculations=[
        metrics.period_to_date(aggregate="min", period="year", alias="this_year_min"),
        metrics.period_to_date(aggregate="max", period="year"),
        metrics.period_to_date(aggregate="sum", period="year"),
        metrics.period_to_date(aggregate="average", period="year"),
    ]
    )
}}
"""

# models/period_to_date_count.yml
period_to_date_count_yml = """
version: 2 
models:
  - name: period_to_date_count
    tests: 
      - metrics.metric_equality:
          compare_model: ref('period_to_date_count__expected')
metrics:
  - name: period_to_date_count
    model: ref('fact_orders')
    label: Count
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/period_to_date_count__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    period_to_date_count__expected_csv = """
date_month,date_year,period_to_date_count,period_to_date_count_this_year_min,period_to_date_count_max_for_year,period_to_date_count_sum_for_year,period_to_date_count_average_for_year
2022-01-01,2022-01-01,7,7,7,7,7.000000
2022-02-01,2022-01-01,3,3,7,10,5.000000
""".lstrip()
else:
    period_to_date_count__expected_csv = """
date_month,date_year,period_to_date_count,period_to_date_count_this_year_min,period_to_date_count_max_for_year,period_to_date_count_sum_for_year,period_to_date_count_average_for_year
2022-01-01,2022-01-01,7,7,7,7,7.0000000000000000
2022-02-01,2022-01-01,3,3,7,10,5.0000000000000000
""".lstrip()

# seeds/period_to_date_count__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    period_to_date_count__expected_yml = """
version: 2
seeds:
  - name: period_to_date_count__expected
    config:
      column_types:
        date_month: date
        date_year: date
        period_to_date_count: INT64
        period_to_date_count_this_year_min: INT64
        period_to_date_count_max_for_year: INT64
        period_to_date_count_sum_for_year: INT64
        period_to_date_count_average_for_year: FLOAT64
""".lstrip()
else: 
    period_to_date_count__expected_yml = """"""

class TestPeriodToDateCount:

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
            "period_to_date_count__expected.csv": period_to_date_count__expected_csv,
            "period_to_date_count__expected.yml": period_to_date_count__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "period_to_date_count.sql": period_to_date_count_sql,
            "period_to_date_count.yml": period_to_date_count_yml
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

# models/period_to_date_derived_metric.sql
period_to_date_derived_metric_sql = """
select *
from 
{{ metrics.calculate(metric('period_to_date_derived_metric'), 
    grain='month',
    secondary_calculations=[
        metrics.period_to_date(aggregate="min", period="year", alias="this_year_min"),
        metrics.period_to_date(aggregate="max", period="year"),
        metrics.period_to_date(aggregate="sum", period="year")
    ]
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

# models/period_to_date_derived_metric.yml
period_to_date_derived_metric_yml = """
version: 2 
models:
  - name: period_to_date_derived_metric
    tests: 
      - metrics.metric_equality:
          compare_model: ref('period_to_date_derived_metric__expected')
metrics:
  - name: period_to_date_derived_metric
    label: derived ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{metric('base_sum_metric')}} + 1"
    dimensions:
      - had_discount
      - order_country
"""

# seeds/period_to_date_derived_metric__expected.csv
period_to_date_derived_metric__expected_csv = """
date_month,date_year,base_sum_metric,period_to_date_derived_metric,period_to_date_derived_metric_this_year_min,period_to_date_derived_metric_max_for_year,period_to_date_derived_metric_sum_for_year
2022-01-01,2022-01-01,8,9,9,9,9
2022-02-01,2022-01-01,6,7,7,9,16
""".lstrip()

# seeds/period_to_date_derived_metric__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    period_to_date_derived_metric__expected_yml = """
version: 2
seeds:
  - name: period_to_date_derived_metric__expected
    config:
      column_types:
        date_month: date
        date_year: date
        period_to_date_derived_metric: INT64
        period_to_date_derived_metric_this_year_min: INT64
        period_to_date_derived_metric_max_for_year: INT64
        period_to_date_derived_metric_sum_for_year: INT64
""".lstrip()
else: 
    period_to_date_derived_metric__expected_yml = """"""

class TestPeriodToDateDerivedMetric:

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
            "period_to_date_derived_metric__expected.csv": period_to_date_derived_metric__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.yml": fact_orders_yml,
            "base_sum_metric.yml": base_sum_metric_yml,
            "period_to_date_derived_metric.yml": period_to_date_derived_metric_yml,
            "fact_orders.sql": fact_orders_sql,
            "period_to_date_derived_metric.sql": period_to_date_derived_metric_sql
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

# models/period_to_date_max.sql
period_to_date_max_sql = """
select *
from 
{{ metrics.calculate(metric('period_to_date_max'), 
    grain='month',
    secondary_calculations=[
        metrics.period_to_date(aggregate="min", period="year", alias="this_year_min"),
        metrics.period_to_date(aggregate="max", period="year"),
        metrics.period_to_date(aggregate="sum", period="year"),
        metrics.period_to_date(aggregate="average", period="year"),
    ]
    )
}}
"""

# models/period_to_date_max.yml
period_to_date_max_yml = """
version: 2 
models:
  - name: period_to_date_max
    tests: 
      - metrics.metric_equality:
          compare_model: ref('period_to_date_max__expected')
metrics:
  - name: period_to_date_max
    model: ref('fact_orders')
    label: max value
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: max
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/period_to_date_max__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    period_to_date_max__expected_csv = """
date_month,date_year,period_to_date_max,period_to_date_max_this_year_min,period_to_date_max_max_for_year,period_to_date_max_sum_for_year,period_to_date_max_average_for_year
2022-01-01,2022-01-01,5,5,5,5,5.000000
2022-02-01,2022-01-01,3,3,5,8,4.000000
""".lstrip()
else:
    period_to_date_max__expected_csv = """
date_month,date_year,period_to_date_max,period_to_date_max_this_year_min,period_to_date_max_max_for_year,period_to_date_max_sum_for_year,period_to_date_max_average_for_year
2022-01-01,2022-01-01,5,5,5,5,5.0000000000000000
2022-02-01,2022-01-01,3,3,5,8,4.0000000000000000
""".lstrip()

# seeds/period_to_date_max__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    period_to_date_max__expected_yml = """
version: 2
seeds:
  - name: period_to_date_max__expected
    config:
      column_types:
        date_month: date
        date_year: date
        period_to_date_max: INT64
        period_to_date_max_this_year_min: INT64
        period_to_date_max_max_for_year: INT64
        period_to_date_max_sum_for_year: INT64
        period_to_date_max_average_for_year: FLOAT64
""".lstrip()
else: 
    period_to_date_max__expected_yml = """"""

class TestPeriodToDateMax:

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
            "period_to_date_max__expected.csv": period_to_date_max__expected_csv,
            "period_to_date_max__expected.yml": period_to_date_max__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "period_to_date_max.sql": period_to_date_max_sql,
            "period_to_date_max.yml": period_to_date_max_yml
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

# models/period_to_date_min.sql
period_to_date_min_sql = """
select *
from 
{{ metrics.calculate(metric('period_to_date_min'), 
    grain='month',
    secondary_calculations=[
        metrics.period_to_date(aggregate="min", period="year", alias="this_year_min"),
        metrics.period_to_date(aggregate="max", period="year"),
        metrics.period_to_date(aggregate="sum", period="year"),
        metrics.period_to_date(aggregate="average", period="year"),
    ]
    )
}}
"""

# models/period_to_date_min.yml
period_to_date_min_yml = """
version: 2 
models:
  - name: period_to_date_min
    tests: 
      - metrics.metric_equality:
          compare_model: ref('period_to_date_min__expected')
metrics:
  - name: period_to_date_min
    model: ref('fact_orders')
    label: min value
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: min
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/period_to_date_min__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    period_to_date_min__expected_csv = """
date_month,date_year,period_to_date_min,period_to_date_min_this_year_min,period_to_date_min_max_for_year,period_to_date_min_sum_for_year,period_to_date_min_average_for_year
2022-01-01,2022-01-01,1,1,1,1,1.000000
2022-02-01,2022-01-01,1,1,1,2,1.000000
""".lstrip()
else:
    period_to_date_min__expected_csv = """
date_month,date_year,period_to_date_min,period_to_date_min_this_year_min,period_to_date_min_max_for_year,period_to_date_min_sum_for_year,period_to_date_min_average_for_year
2022-01-01,2022-01-01,1,1,1,1,1.0000000000000000
2022-02-01,2022-01-01,1,1,1,2,1.0000000000000000
""".lstrip()

# seeds/period_to_date_min__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    period_to_date_min__expected_yml = """
version: 2
seeds:
  - name: period_to_date_min__expected
    config:
      column_types:
        date_month: date
        date_year: date
        period_to_date_min: INT64
        period_to_date_min_this_year_min: INT64
        period_to_date_min_max_for_year: INT64
        period_to_date_min_sum_for_year: INT64
        period_to_date_min_average_for_year: FLOAT64
""".lstrip()
else: 
    period_to_date_min__expected_yml = """"""

class TestPeriodToDateMin:

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
            "period_to_date_min__expected.csv": period_to_date_min__expected_csv,
            "period_to_date_min__expected.yml": period_to_date_min__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "period_to_date_min.sql": period_to_date_min_sql,
            "period_to_date_min.yml": period_to_date_min_yml
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

# models/same_period_and_grain.sql
same_period_and_grain_sql = """
select *
from 
{{ metrics.calculate(metric('same_period_and_grain'), 
    grain='day',
    secondary_calculations=[
        metrics.period_to_date(aggregate="sum", period="day",alias="day_sum")
    ]
    )
}}
"""

# models/same_period_and_grain.yml
same_period_and_grain_yml = """
version: 2 
models:
  - name: same_period_and_grain
    tests: 
      - metrics.metric_equality:
          compare_model: ref('same_period_and_grain__expected')
metrics:
  - name: same_period_and_grain
    model: ref('fact_orders')
    label: Count
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/same_period_and_grain__expected.csv
same_period_and_grain__expected_csv = """
date_day,same_period_and_grain,same_period_and_grain_day_sum
2022-02-03,1,1
2022-01-12,0,0
2022-01-07,0,0
2022-02-01,0,0
2022-01-08,1,1
2022-02-10,0,0
2022-01-28,1,1
2022-01-14,0,0
2022-02-15,1,1
2022-02-08,0,0
2022-01-21,1,1
2022-02-13,1,1
2022-02-04,0,0
2022-01-17,0,0
2022-02-09,0,0
2022-01-13,1,1
2022-02-06,0,0
2022-01-11,0,0
2022-02-12,0,0
2022-01-16,0,0
2022-02-05,0,0
2022-01-15,0,0
2022-01-23,0,0
2022-01-06,1,1
2022-01-26,0,0
2022-01-22,1,1
2022-01-19,0,0
2022-01-25,0,0
2022-01-09,0,0
2022-02-14,0,0
2022-01-10,0,0
2022-01-30,0,0
2022-02-11,0,0
2022-01-27,0,0
2022-01-29,0,0
2022-01-24,0,0
2022-01-31,0,0
2022-01-20,1,1
2022-01-18,0,0
2022-02-02,0,0
2022-02-07,0,0
""".lstrip()

class TestSamePeriodAndGrainCount:

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
            "same_period_and_grain__expected.csv": same_period_and_grain__expected_csv
            }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "same_period_and_grain.sql": same_period_and_grain_sql,
            "same_period_and_grain.yml": same_period_and_grain_yml
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

# models/period_to_date_sum.sql
period_to_date_sum_sql = """
select *
from 
{{ metrics.calculate(metric('period_to_date_sum'), 
    grain='month',
    secondary_calculations=[
        metrics.period_to_date(aggregate="sum", period="year", alias="this_year_sum"),
        metrics.period_to_date(aggregate="max", period="year"),
        metrics.period_to_date(aggregate="min", period="year"),
        metrics.period_to_date(aggregate="average", period="year"),
    ]
    )
}}
"""

# models/period_to_date_sum.yml
period_to_date_sum_yml = """
version: 2 
models:
  - name: period_to_date_sum
    tests: 
      - metrics.metric_equality:
          compare_model: ref('period_to_date_sum__expected')
metrics:
  - name: period_to_date_sum
    model: ref('fact_orders')
    label: sum value
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: customer_id
    dimensions:
      - had_discount
      - order_country
"""

# seeds/period_to_date_sum__expected.csv
if os.getenv('dbt_target') == 'snowflake':
    period_to_date_sum__expected_csv = """
date_month,date_year,period_to_date_sum,period_to_date_sum_this_year_sum,period_to_date_sum_max_for_year,period_to_date_sum_min_for_year,period_to_date_sum_average_for_year
2022-01-01,2022-01-01,18,18,18,18,18.000000
2022-02-01,2022-01-01,6,24,18,6,12.000000
""".lstrip()
else:
    period_to_date_sum__expected_csv = """
date_month,date_year,period_to_date_sum,period_to_date_sum_this_year_sum,period_to_date_sum_max_for_year,period_to_date_sum_min_for_year,period_to_date_sum_average_for_year
2022-01-01,2022-01-01,18,18,18,18,18.0000000000000000
2022-02-01,2022-01-01,6,24,18,6,12.0000000000000000
""".lstrip()

# seeds/period_to_date_sum__expected.yml
if os.getenv('dbt_target') == 'bigquery':
    period_to_date_sum__expected_yml = """
version: 2
seeds:
  - name: period_to_date_sum__expected
    config:
      column_types:
        date_month: date
        date_year: date
        period_to_date_sum: INT64
        period_to_date_sum_this_year_sum: INT64
        period_to_date_sum_max_for_year: INT64
        period_to_date_sum_min_for_year: INT64
        period_to_date_sum_average_for_year: FLOAT64
""".lstrip()
else: 
    period_to_date_sum__expected_yml = """"""
    

class TestPeriodToDateSum:

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
            "period_to_date_sum__expected.csv": period_to_date_sum__expected_csv,
            "period_to_date_sum__expected.yml": period_to_date_sum__expected_yml
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "period_to_date_sum.sql": period_to_date_sum_sql,
            "period_to_date_sum.yml": period_to_date_sum_yml
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