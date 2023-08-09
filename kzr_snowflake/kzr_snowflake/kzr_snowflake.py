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
import sqlparse
import glob
import os
from datetime import datetime

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


def connect(externalbrowser: bool = False):
    global connection
    with open(path, "r") as f:
        cred = json.load(f)
    if externalbrowser:
        connection = snowflake.connector.connect(
            authenticator='externalbrowser',
            user=cred['user'],
            account=cred['account'],
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            paramstyle='qmark'
        )
    else:
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
    with connection.cursor() as cs:
        try:
            cs.execute(statement)
        except Exception as e:
            print(e)
            return False
        finally:
            if not cs.messages:
                print("Executed")
                return True
    return False


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


class ETL_Table:
    column_details = [
        ('script_id', int, -1),
        ('folder_level', int, -1),
        ('parent_folder_name', str, ''),
        ('folder_name', str, ''),
        ('folder_order', int, -1),
        ('sql_file_name', str, ''),
        ('sql_file_order', int, -1),
        ('script_id', int, -1),
        ('sql_command', str, ''),
        ('sql_command_order', int, -1),
        ('is_executed', bool, False),
        ('exec_time', str, -1),
        ('path', str, ''),
        ('is_dir', bool, False),
        ('size', float, -1.0)
    ]

    def __init__(self, **kwargs):
        # set the attribute values based on the keyword arguments
        for col, dtype, default_val in self.column_details:
            setattr(self, col, kwargs.get(col, default_val))

    def copy(self):
        # create a new instance of TableDetails with the same attribute values
        new_instance = ETL_Table()
        for col, _, _ in self.column_details:
            setattr(new_instance, col, getattr(self, col))
        return new_instance


def get_sql_commands_from_file(sql_file_path):
    return_val = []
    with open(sql_file_path, 'r') as sql_file:
        sql_list = sqlparse.split(sql_file.read())
        sql_order = 1

        for sql in sql_list:
            sql = sqlparse.format(sql, strip_comments=True).strip()
            if sql:
                return_val.append({
                    "SQL_COMMAND": sql,
                    "SQL_COMMAND_ORDER": sql_order
                })
                sql_order += 1
    return return_val


def create_etl_from_source_code(folder_path, etl_table_name, add_datetime=False):
    try:
        if add_datetime is True:
            # Get the current date
            current_date = datetime.now()

            # Format the date as YYYYMMDD
            formatted_date = current_date.strftime("%Y%m%d")
            etl_table_name = f'{etl_table_name}_{formatted_date}'

        # Traverse the folder structure using glob and add each file/folder to the table
        root_name = 'ROOT'
        table_details_list = []
        for sub_folder in sorted(glob.glob(f'{folder_path}**', recursive=True)):
            td = ETL_Table()
            if sub_folder == folder_path:
                continue

            td.path = f'{root_name}/{sub_folder.split(folder_path)[1]}'
            parth_arr = td.path.split('/')
            td.folder_level = len(parth_arr)
            if td.folder_level <= 1:
                continue

            td.is_dir = os.path.isdir(sub_folder)
            folder_path_pos = -1
            sql_command_list = []

            if not td.is_dir:
                if parth_arr[-1][-4:] != '.sql':
                    continue
                td.size = os.path.getsize(sub_folder)
                folder_path_pos = -2
                sql_file_order, _, td.sql_file_name = parth_arr[-1].partition('_')
                if not sql_file_order.isnumeric():
                    sql_file_order = -1
                    td.sql_file_name = parth_arr[-1]
                td.sql_file_order = int(sql_file_order)
                sql_command_list = get_sql_commands_from_file(sub_folder)

            td.parent_folder_name = parth_arr[folder_path_pos - 1] if td.folder_level > 2 else root_name
            folder_order, _, td.folder_name = parth_arr[folder_path_pos].partition('_')

            if not folder_order.isnumeric():
                folder_order = -1
                td.folder_name = parth_arr[folder_path_pos]
            td.folder_order = int(folder_order)

            if len(sql_command_list) > 0:
                for sql_commands in sql_command_list:
                    td.sql_command = sql_commands['SQL_COMMAND']
                    td.sql_command_order = sql_commands['SQL_COMMAND_ORDER']
                    table_details_list.append(td.copy())
            else:
                # table_details_list.append(td)
                print('skip folders')

        # convert the list to a Pandas DataFrame
        etl_df = pd.DataFrame([vars(table_details) for table_details in table_details_list])
        etl_df['script_id'] = etl_df.index + 1
        etl_df.columns = etl_df.columns.str.upper()

        # Create table for transformation scripts
        st = f'''
            create or replace table {etl_table_name}
        (
            SCRIPT_ID          numeric                     not null,
            FOLDER_LEVEL       numeric                     not null,
            PARENT_FOLDER_NAME varchar(100)                not null,
            FOLDER_NAME        varchar(100)                not null,
            FOLDER_ORDER       numeric(10)   default -1    not null,
            SQL_FILE_NAME      varchar(100)                not null,
            SQL_FILE_ORDER     numeric(10)   default -1    not null,
            SQL_COMMAND        text                        not null,
            SQL_COMMAND_ORDER  numeric(10)   default -1    not null,
            IS_EXECUTED        boolean       default false not null,
            EXEC_TIME          number(30, 8) default -1    not null,
            path               text                        not null,
            is_dir             boolean                     not null,
            size               real                        not null
        );
        '''
        execute(st)
        insert_df(etl_table_name, etl_df)

    except Exception as e:
        print(e)


def create_task(
    name
    , sql
    , warehouse=False
    , user_task_managed_initial_warehouse_size=False
    , schedule=False
    , allow_overlapping_execution=False
    , user_task_timeout_ms=False
    , comment=False
    , after=False
    , when=False
    , replace=False
    , print_statement=False
):
    if (warehouse and user_task_managed_initial_warehouse_size) or (
        not warehouse and not user_task_managed_initial_warehouse_size):
        print('Use one type of warehouse option!')
        return False

    if (schedule and after) or (not schedule and not after):
        print('Choose either task is after another task or provide schedule!')
        return False

    statement = f"CREATE TASK {name}" if not replace else f"CREATE OR REPLACE TASK {name}"
    statement += f"\n\tUSER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = '{user_task_managed_initial_warehouse_size}" if not warehouse else f"\n\tWAREHOUSE = {warehouse}"
    statement += f"\n\tSCHEDULE = '{schedule}'" if not after else f"\n\tAFTER = '{after}'"
    if allow_overlapping_execution:
        statement += f"\n\tALLOW_OVERLAPPING_EXECUTION = TRUE"
    if user_task_timeout_ms:
        statement += f"\n\tUSER_TASK_TIMEOUT_MS = {user_task_timeout_ms}"
    if comment:
        statement += f"\n\tCOMMENT = {comment}"
    if when:
        statement += f"\nWHEN\n\t{when}"
    statement += f"\nAS\n\t{sql};"

    if print_statement: print(statement)
    return execute(statement)


def float_max_precision_and_scale(series):
    try:
        split_series = series.dropna().astype(str).str.split('.', expand=True)
        max_precision_before_dot = split_series[0].str.len().max()
        max_precision_after_dot = split_series[1].str.len().max() if split_series.shape[1] > 1 else 0
        return {'precision': max_precision_before_dot + max_precision_after_dot, 'scale': max_precision_after_dot}
    except Exception as e:
        print(f"Error in float_max_precision_and_scale: {e}")
        return {'precision': 0, 'scale': 0}


def map_dataframe_to_snowflake(df):
    column_mappings = {}
    try:
        for column, dtype in df.dtypes.items():
            if dtype == 'float64':
                specs = float_max_precision_and_scale(df[column])
                column_mappings[column] = f"NUMBER({specs['precision']},{specs['scale']})"
            elif dtype == 'int64':
                precision = df[column].dropna().astype(str).str.len().max()
                column_mappings[column] = f"NUMBER({precision},0)"
            elif dtype == 'bool':
                column_mappings[column] = "BOOLEAN"
            elif dtype == 'object':
                max_len = df[column].dropna().astype(str).str.len().max()
                column_mappings[column] = f"VARCHAR({max_len})"
            elif dtype.name.startswith("datetime"):
                column_mappings[column] = "TIMESTAMP_NTZ"
            else:
                column_mappings[column] = "VARIANT"
    except Exception as e:
        print(f"Error in map_dataframe_to_snowflake: {e}")
    return column_mappings


def create_statement_from_df(table_name, df):
    try:
        mappings = map_dataframe_to_snowflake(df)
        columns_definition = ', '.join([f"{col} {dtype}" for col, dtype in mappings.items()])
        create_statement = f"CREATE OR REPLACE TABLE {table_name} ({columns_definition})"
        return create_statement
    except Exception as e:
        print(f"Error in create_statement_from_df: {e}")
        return ""


def create_table_df(table_name, df, create_statement=False):
    try:
        if not create_statement:
            create_statement = create_statement_from_df(table_name, df)
        execute(create_statement)
        insert_df(table_name, df)
    except Exception as e:
        print(f"Error: {e}")
