import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here

# We use os.environ here instead of os.getenv because environ with [] input will
# return a KeyError exception instead of None or Default Value. It's better to know
# when the error is from the environment variables and not have it potentially lead
# you down a red herring path with other issues.
@pytest.fixture(scope="class")
def dbt_profile_target():
    
    if os.environ['dbt_target'] == 'postgres':
        return {
            'type': 'postgres',
            'threads': 1,
            'host': os.environ['POSTGRES_TEST_HOST'],
            'user': os.environ['POSTGRES_TEST_USER'],
            'password': os.environ['POSTGRES_TEST_PASSWORD'],
            'port': int(os.environ['POSTGRES_TEST_PORT']),
            'database': os.environ['POSTGRES_TEST_DB'],
        }

    if os.environ['dbt_target'] == 'redshift':
        return {
            'type': 'redshift',
            'threads': 1,
            'host': os.environ['REDSHIFT_TEST_HOST'],
            'user': os.environ['REDSHIFT_TEST_USER'],
            'pass': os.environ['REDSHIFT_TEST_PASS'],
            'dbname': os.environ['REDSHIFT_TEST_DBNAME'],
            'port': int(os.environ['REDSHIFT_TEST_PORT']),
        }

    if os.environ['dbt_target'] == 'snowflake':
        return {
            'type': 'snowflake',
            'threads': 1,
            'account': os.environ['SNOWFLAKE_TEST_ACCOUNT'],
            'user': os.environ['SNOWFLAKE_TEST_USER'],
            'password': os.environ['SNOWFLAKE_TEST_PASSWORD'],
            'role': os.environ['SNOWFLAKE_TEST_ROLE'],
            'database': os.environ['SNOWFLAKE_TEST_DATABASE'],
            'warehouse': os.environ['SNOWFLAKE_TEST_WAREHOUSE'],
        }

    if os.environ['dbt_target'] == 'bigquery':
        return {
            'type': 'bigquery',
            'threads': 1,
            'method': 'service-account',
            'project': os.environ['BIGQUERY_TEST_PROJECT'],
            'keyfile': os.environ['BIGQUERY_SERVICE_KEY_PATH'],
        }