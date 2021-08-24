# pip install -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.5.0/tested_requirements/requirements_36.reqs
# pip install snowflake-connector-python==2.5.0
from numpy.distutils.fcompiler import none
import json
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas
import timeit
import pandas as pd
import pyarrow
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

connection: SnowflakeConnection = none
path = ""
role = ""
warehouse = ""
database = ""
schema = ""


def get_engine():
    with open(path, "r") as f:
        cred = json.load(f)
    engine = create_engine(URL(
        user=cred['user'],
        password=cred['password'],
        account=cred['account'],
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema
    ))
    conn = engine.connect()
    return engine


def connect():
    global connection
    with open(path, "r") as f:
        cred = json.load(f)
    connection = snowflake.connector.connect(
        user=cred['user'],
        password=cred['password'],
        account=cred['account'],
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
        paramstyle='qmark'
    )
    print("connected!")


def execute(statement):
    cs = connection.cursor()
    try:
        cs.execute(statement)
    except Exception as e:
        print(e)
    finally:
        if not cs.messages:
            print("Executed")


def disconnect():
    connection.close()


def select_m(statement):
    print(timeit.timeit("pass"))
    cs = connection.cursor()
    df = none
    try:
        cs.execute(statement)
        df = cs.fetch_pandas_all()
    except Exception as e:
        print(e)
    finally:
        cs.close()
        print(timeit.timeit("pass"))
        return df


def select_one(statement):
    cs = connection.cursor()
    value = none
    try:
        cs.execute(statement)
        value = cs.fetchone()
    except Exception as e:
        print(e)
    finally:
        cs.close()
        print(timeit.timeit("pass"))
        return value


def insert_m(statement, params):
    cs = connection.cursor()
    try:
        cs.executemany(statement, params)
    except Exception as e:
        print(e)
    finally:
        cs.close()


def select_into_df(statement):
    try:
        df = pd.read_sql(
            statement,
            connection
        )
    except Exception as e:
        print(e)
    finally:
        return df


def insert_df(table_name, df):
    success = False
    try:
        success, nchunks, nrows, output = write_pandas(
            connection,
            df,
            table_name
        )
    except Exception as e:
        print('error: {}'.format(e))
    finally:
        if success:
            print(
                'success = ' + str(success)
                + '\nnchunks = ' + str(nchunks)
                + '\nnrows = ' + str(nrows)
                + '\nnrows = ' + str(nrows)
            )
