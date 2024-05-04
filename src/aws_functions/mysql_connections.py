import sys
import os
import pymysql
from pandas import DataFrame

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(parent_dir)

from aux_functions.log_function import LoggerJob

log = LoggerJob().log()


def connection_with_rds_mysql(
    rds_proxy_host: str,
    user_name: str,
    password: str,
    db_name: str,
    connect_timeout_paramenter: str = 5,
) -> pymysql.connections.Connection:
    """
    Establish a connection with AWS RDS MySQL.

    Args:
        rds_proxy_host (str): IP address of the RDS proxy.
        user_name (str): The username for accessing the MySQL database.
        password (str): The password for accessing the MySQL database.
        db_name (str): The name of the database to connect to.
        connect_timeout_parameter (str): The timeout value for establishing the
            connection (default is '5' seconds).

    Returns:
        pymysql.connections.Connection: A connection object representing the connection
            established with the target MySQL database.

    Raises:
        pymysql.MySQLError: If an error occurs during the connection process, such as
            inability to connect or invalid credentials.
    """
    log.info("Starting connection to RDS")
    try:
        conn = pymysql.connect(
            host=rds_proxy_host,
            user=user_name,
            passwd=password,
            db=db_name,
            connect_timeout=connect_timeout_paramenter,
        )
        log.info("Connection with RDS MySQL successful")

    except pymysql.MySQLError as e:
        log.error(
            f"ERROR: Unexpected error: Could not connect to MySQL instance. \n {e}"
        )
        sys.exit(1)

    return conn


def query_execution_python(
    conn: pymysql.connections.Connection, query: str, query_type: str
) -> dict | None:
    """
    Execute the query on the specifict Mysql database.

    Args:
        conn (pymysql.connections.Connection): A connection object representing the
            connection established with the target MySQL database.
        query (str): Contain the query to be executed.
        query_type (str): Type of actions which will be used.

    Returns:
        dict or None: If 'query_type' is 'read', returns a pandas DataFrame containing
            the result set of the executed query. If 'query_type' is 'write', returns
                None.

    Raises:
        pymysql.MySQLError: If an error occurs during the connection process, such as
            inability to connect or invalid credentials.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            if query_type == "read":
                log.info("Read mode activated")
                df = get_table_convert_dict(cur)
                return df

            else:
                conn.commit()
                log.info("Query executed successfully")
                return None
    except Exception as e:
        log.error(f"Error executing query: {e}")
        return None


def get_table_convert_dict(cur):

    try:
        data = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        data_as_dicts = list(map(lambda row: dict(zip(column_names, row)), data))
        data_as_dicts = DataFrame(data_as_dicts)

        return data_as_dicts

    except Exception as e:
        log.error(f"Error converting data to dictionary: {e}")
        return None
