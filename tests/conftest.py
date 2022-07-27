import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    
    if os.getenv('dbt_target') == 'postgres':
        return {
            'type': 'postgres',
            'threads': 1,
            'host': os.getenv('POSTGRES_TEST_HOST'),
            'user': os.getenv('POSTGRES_TEST_USER'),
            'password': os.getenv('POSTGRES_TEST_PASS'),
            'port': int(os.getenv('POSTGRES_TEST_PORT')),
            'database': os.getenv('POSTGRES_TEST_DBNAME'),
        }

    if os.getenv('dbt_target') == 'snowflake':
        return {
            'type': 'snowflake',
            'threads': 1,
            'account': os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_TEST_USER'),
            'password': os.getenv('SNOWFLAKE_TEST_PASSWORD'),
            'role': os.getenv('SNOWFLAKE_TEST_ROLE'),
            'database': os.getenv('SNOWFLAKE_TEST_DATABASE'),
            'warehouse': os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
        }

    if os.getenv('dbt_target') == 'redshift':
        return {
            'type': 'redshift',
            'threads': 1,
            'host': os.getenv('POSTGRES_TEST_HOST'),
            'user': os.getenv('REDSHIFT_TEST_USER'),
            'pass': os.getenv('REDSHIFT_TEST_PASS'),
            'dbname': os.getenv('REDSHIFT_TEST_DBNAME'),
            'port': os.getenv('REDSHIFT_TEST_PORT'),
        }

    if os.getenv('dbt_target') == 'bigquery':
        return {
            'type': 'bigquery',
            'threads': 1,
            'method': 'service-account',
            'keyfile': os.getenv('BIGQUERY_SERVICE_KEY_PATH'),
            'project': os.getenv('BIGQUERY_TEST_DATABASE'),
        }