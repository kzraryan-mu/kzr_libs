import pandas as pd
import cx_Oracle
from cx_Oracle import Connection
from numpy.distutils.fcompiler import none
import json
import time

# Default variables
connection: Connection = none
path = ""


# Database Connection
def connect():
    global connection
    with open(path, "r") as f:
        cred = json.load(f)
    connection = cx_Oracle.connect(
        user=cred['user']
        , password=cred['password']
        , dsn=cred['dsn']
        , encoding="UTF-8"
    )
    print("connected!")


def disconnect():
    connection.close()


# Execute a query
def execute(statement):
    cs = connection.cursor()
    try:
        cs.execute(statement)
    except Exception as e:
        print(e)
    finally:
        if not cs.messages:
            print("Executed")


# Select statements
def select_m(statement):
    cs = connection.cursor()
    df = none
    try:
        cs.execute(statement)
        df = cs.fetch_pandas_all()
    except Exception as e:
        print(e)
    finally:
        cs.close()
        return df


def select_with_structure(statement):
    start = time.time() * 1000
    cs = connection.cursor()
    df = none
    ddl = """
        create table 
    """
    try:
        cs.execute(statement)
        # print(time.time() * 1000 - start)

        cols = [field[0] for field in cs.description]
        print(cs.description)
        rows = cs.fetchall()
        # print(time.time() * 1000 - start)
        df = pd.DataFrame(rows, columns=cols, dtype=str)
        # print(time.time() * 1000 - start)
    except Exception as e:
        print(e)
    finally:
        cs.close()
        # print(time.time() * 1000 - start)
        return df

def select_a(statement):
    start = time.time() * 1000
    cs = connection.cursor()
    df = none
    try:
        cs.execute(statement)
        print(time.time() * 1000 - start)
        cols = [field[0] for field in cs.description]
        rows = cs.fetchall()
        print(time.time() * 1000 - start)
        df = pd.DataFrame(rows, columns=cols)
        print(time.time() * 1000 - start)
    except Exception as e:
        print(e)
    finally:
        cs.close()
        print(time.time() * 1000 - start)
        return df


def select_cs(statement):
    start = time.time() * 1000
    cs = connection.cursor()
    try:
        cs.execute(statement)
        print(time.time() * 1000 - start)
    except Exception as e:
        print(e)
    finally:
        return cs


def select_m2(statement):
    start = time.time()*1000
    df = none
    try:
        df = pd.read_sql(statement, con=connection)
    except Exception as e:
        print(e)
    finally:
        print(time.time()*1000 - start)
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
        return value
