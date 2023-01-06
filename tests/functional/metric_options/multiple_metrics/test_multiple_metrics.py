from struct import pack
import os
import pytest
from dbt.tests.util import run_dbt

# our file contents
from tests.functional.fixtures import (
    fact_orders_source_csv,
    fact_orders_sql,
    fact_orders_yml,
    event_sql,
    event_yml,
    events_source_csv
)

# models/multiple_metrics.sql
multiple_metrics_sql = """
select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric'), metric('base_count_metric')],
    grain='month'
    )
}}
"""

# models/multiple_metrics.yml
multiple_metrics_yml = """
version: 2 
models:
  - name: multiple_metrics
    tests: 
      - metrics.metric_equality:
          compare_model: ref('multiple_metrics__expected')
metrics:
  - name: base_count_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count
    expression: order_total
    dimensions:
      - had_discount
      - order_country

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

# seeds/multiple_metrics__expected.csv
multiple_metrics__expected_csv = """
date_month,base_sum_metric,base_count_metric
2022-01-01,8,7
2022-02-01,6,3
""".lstrip()

class TestMultipleMetrics:

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
            "multiple_metrics__expected.csv": multiple_metrics__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "multiple_metrics.sql": multiple_metrics_sql,
            "multiple_metrics.yml": multiple_metrics_yml
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

# models/multiple_metrics.sql
multiple_metrics_sql = """
select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric'), metric('base_count_metric')],
    grain='month',
    dimensions=['had_discount']
    )
}}
"""

# models/multiple_metrics.yml
multiple_metrics_yml = """
version: 2 
models:
  - name: multiple_metrics
    tests: 
      - metrics.metric_equality:
          compare_model: ref('multiple_metrics__expected')
metrics:
  - name: base_count_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: count
    expression: order_total
    dimensions:
      - had_discount
      - order_country

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

# seeds/multiple_metrics__expected.csv
multiple_metrics__expected_csv = """
date_month,had_discount,base_sum_metric,base_count_metric
2022-01-01,TRUE,2,2
2022-01-01,FALSE,6,5
2022-02-01,TRUE,4,1
2022-02-01,FALSE,2,2
""".lstrip()

class TestMultipleMetricsWithDimension:

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
            "multiple_metrics__expected.csv": multiple_metrics__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "multiple_metrics.sql": multiple_metrics_sql,
            "multiple_metrics.yml": multiple_metrics_yml
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

# models/multiple_metrics.sql
multiple_metrics_sql = """
select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric'), metric('base_count_metric')],
    grain='month',
    secondary_calculations=[
    metrics.period_over_period(
        comparison_strategy="difference"
        ,interval=1
        ,metric_list=['base_sum_metric']
        ),
    metrics.period_to_date(
        aggregate="sum"
        ,period="year"
        ,metric_list=['base_sum_metric','base_count_metric']
        ),
    metrics.rolling(
        aggregate="max"
        ,interval=4
        ,metric_list='base_sum_metric'
        )
        ] 
    )
}}
"""

# models/multiple_metrics.yml
multiple_metrics_yml = """
version: 2 
models:
  - name: multiple_metrics
    tests: 
      - metrics.metric_equality:
          compare_model: ref('multiple_metrics__expected')
metrics:
  - name: base_count_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: count
    sql: order_total
    dimensions:
      - had_discount
      - order_country
  - name: base_sum_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    type: sum
    sql: order_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/multiple_metrics__expected.csv
multiple_metrics__expected_csv = """
date_month,date_year,base_sum_metric,base_count_metric,base_sum_metric_difference_to_1_month_ago,base_sum_metric_sum_for_year,base_count_metric_sum_for_year,base_sum_metric_rolling_max_4_month
2022-01-01,2022-01-01,8,7,8,8,7,8
2022-02-01,2022-01-01,6,3,-2,14,10,8
""".lstrip()

class TestMultipleMetricsSecondaryCalcs:

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
            "multiple_metrics__expected.csv": multiple_metrics__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "multiple_metrics.sql": multiple_metrics_sql,
            "multiple_metrics.yml": multiple_metrics_yml
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


# models/multiple_metrics.sql
multiple_metrics_sql = """
select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric'), metric('derived_metric')],
    grain='month'
    )
}}
"""

# models/multiple_metrics.yml
multiple_metrics_yml = """
version: 2 
models:
  - name: multiple_metrics
    tests: 
      - metrics.metric_equality:
          compare_model: ref('multiple_metrics__expected')
metrics:
  - name: derived_metric
    label: derived ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{metric('base_sum_metric')}} + 1"
    dimensions:
      - had_discount
      - order_country

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

# seeds/multiple_metrics__expected.csv
multiple_metrics__expected_csv = """
date_month,base_sum_metric,derived_metric
2022-01-01,8,9
2022-02-01,6,7
""".lstrip()

class TestMultipleMetricsWithDerived:

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
            "multiple_metrics__expected.csv": multiple_metrics__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "multiple_metrics.sql": multiple_metrics_sql,
            "multiple_metrics.yml": multiple_metrics_yml
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

# models/multiple_metrics.sql
multiple_metrics_sql = """
select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric'), metric('derived_metric')],
    grain='month',
    dimensions=['had_discount']
    )
}}
"""

# models/multiple_metrics.yml
multiple_metrics_yml = """
version: 2 
models:
  - name: multiple_metrics
    tests: 
      - metrics.metric_equality:
          compare_model: ref('multiple_metrics__expected')
metrics:
  - name: derived_metric
    label: derived ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{metric('base_sum_metric')}} + 1"
    dimensions:
      - had_discount
      - order_country

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

# seeds/multiple_metrics__expected.csv
multiple_metrics__expected_csv = """
date_month,had_discount,base_sum_metric,derived_metric
2022-01-01,TRUE,2,3
2022-01-01,FALSE,6,7
2022-02-01,TRUE,4,5
2022-02-01,FALSE,2,3
""".lstrip()

class TestMultipleMetricsWithDerivedAndDimension:

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
            "multiple_metrics__expected.csv": multiple_metrics__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "multiple_metrics.sql": multiple_metrics_sql,
            "multiple_metrics.yml": multiple_metrics_yml
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

# models/multiple_metrics_filter.sql
multiple_metrics_filter_sql = """
select *
from {{
  metrics.calculate(
    [
      metric('count_fr_events'),
      metric('count_uk_events'),
    ],
    grain='month',
  )
}}
"""

# models/multiple_metrics_filter.yml
multiple_metrics_filter_yml = """
version: 2
models:
  - name: multiple_metrics_filter
    tests: 
      - metrics.metric_equality:
          compare_model: ref('multiple_metrics_filter__expected')

metrics:
  - name: count_fr_events
    label: Count number of events in France
    model: ref('event')
    type: count
    sql: id
    timestamp: timestamp_field
    time_grains: [month]

    filters:
      - field: country
        operator: '='
        value: "'FR'"

  - name: count_uk_events
    label: Count number of events in UK
    model: ref('event')
    type: count
    sql: id
    timestamp: timestamp_field
    time_grains: [month]

    filters:
      - field: country
        operator: '='
        value: "'UK'"
"""

# seeds/multiple_metrics_filter__expected.csv
multiple_metrics_filter__expected_csv = """
date_month,count_fr_events,count_uk_events
2022-01-01,1,0
2022-02-01,0,1
""".lstrip()

class TestMultipleMetricsWithFilter:

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
            "events_source.csv": events_source_csv,
            "multiple_metrics_filter__expected.csv": multiple_metrics_filter__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "event.sql": event_sql,
            "event.yml": event_yml,
            "multiple_metrics_filter.sql": multiple_metrics_filter_sql,
            "multiple_metrics_filter.yml": multiple_metrics_filter_yml
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

# models/multiple_metrics_no_time_grain.sql
multiple_metrics_no_time_grain_sql = """
select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric_no_time_grain'), metric('base_count_metric_no_time_grain')])
}}
"""

# models/multiple_metrics_no_time_grain.yml
multiple_metrics_no_time_grain_yml = """
version: 2 
models:
  - name: multiple_metrics_no_time_grain
    tests: 
      - metrics.metric_equality:
          compare_model: ref('multiple_metrics_no_time_grain__expected')
metrics:
  - name: base_count_metric_no_time_grain
    model: ref('fact_orders')
    label: Total Discount ($)
    calculation_method: count
    expression: order_total
    dimensions:
      - had_discount
      - order_country

  - name: base_sum_metric_no_time_grain
    model: ref('fact_orders')
    label: Total Discount ($)
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/multiple_metrics_no_time_grain__expected.csv
multiple_metrics_no_time_grain__expected_csv = """
base_sum_metric_no_time_grain,base_count_metric_no_time_grain
14,10
""".lstrip()

class TestMultipleMetricsNoTimeGrain:

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
            "multiple_metrics_no_time_grain__expected.csv": multiple_metrics_no_time_grain__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "multiple_metrics_no_time_grain.sql": multiple_metrics_no_time_grain_sql,
            "multiple_metrics_no_time_grain.yml": multiple_metrics_no_time_grain_yml
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


# models/multiple_metrics_no_time_grain_multiple_dimensions.sql
multiple_metrics_no_time_grain_multiple_dimensions_sql = """
select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric_no_time_grain_multiple_dimensions'), metric('base_count_metric_no_time_grain_multiple_dimensions')],
    dimensions=['had_discount','order_country'])
}}
"""

# models/multiple_metrics_no_time_grain_multiple_dimensions.yml
multiple_metrics_no_time_grain_multiple_dimensions_yml = """
version: 2 
models:
  - name: multiple_metrics_no_time_grain_multiple_dimensions
    tests: 
      - metrics.metric_equality:
          compare_model: ref('multiple_metrics_no_time_grain_multiple_dimensions__expected')
metrics:
  - name: base_count_metric_no_time_grain_multiple_dimensions
    model: ref('fact_orders')
    label: Total Discount ($)
    calculation_method: count
    expression: order_total
    dimensions:
      - had_discount
      - order_country

  - name: base_sum_metric_no_time_grain_multiple_dimensions
    model: ref('fact_orders')
    label: Total Discount ($)
    calculation_method: sum
    expression: order_total
    dimensions:
      - had_discount
      - order_country
"""

# seeds/multiple_metrics_no_time_grain_multiple_dimensions__expected.csv
multiple_metrics_no_time_grain_multiple_dimensions__expected_csv = """
had_discount,order_country,base_sum_metric_no_time_grain_multiple_dimensions,base_count_metric_no_time_grain_multiple_dimensions
true,France,5,2
true,Japan,1,1
false,France,4,3
false,Japan,4,4
""".lstrip()

class TestMultipleMetricsNoTimeGrainMultipleDimensions:

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
            "multiple_metrics_no_time_grain_multiple_dimensions__expected.csv": multiple_metrics_no_time_grain_multiple_dimensions__expected_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fact_orders.sql": fact_orders_sql,
            "fact_orders.yml": fact_orders_yml,
            "multiple_metrics_no_time_grain_multiple_dimensions.sql": multiple_metrics_no_time_grain_multiple_dimensions_sql,
            "multiple_metrics_no_time_grain_multiple_dimensions.yml": multiple_metrics_no_time_grain_multiple_dimensions_yml
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