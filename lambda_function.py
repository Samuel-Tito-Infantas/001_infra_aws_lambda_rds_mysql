from src.aws_functions.autentication import get_secret
from src.aws_functions.mysql_connections import (
    connection_with_rds_mysql,
    query_execution_python,
)
from src.parameters import key_parameters
from src.aux_functions.health_check_function import is_health_check_enabled
from src.aux_functions.log_function import LoggerJob
import json


def lambda_handler(event, context):
    log = LoggerJob().log()
    health_check_enabled = is_health_check_enabled(event)
    if health_check_enabled:
        return {
            "statusCode": 200 if health_check_enabled else 500,
            "body": f"Lambda is {'healthy' if health_check_enabled else 'unhealthy'}",
            "headers": {"Content-Type": "application/json"},
        }

    else:
        execution_type = event.get("action")
        db_name = event.get("data", {}).get("db_name")
        set_enviroment = event.get("environment")
        user_name, password, rds_proxy_host = get_secret(set_enviroment)

        conn = connection_with_rds_mysql(
            rds_proxy_host,
            user_name,
            password,
            db_name,
            connect_timeout_paramenter=5,
        )

        query = f"SELECT * FROM {db_name}.table LIMIT 10"
        query_type = "read"

        # query = f"""INSERT INTO  {db_name}.table
        # (column1, column2, column3, column4, column5, column6, column7 )
        # VALUES (val1, val2, val3, val4, val5, val6, val7)"""
        # query_type = "insert"

        df = query_execution_python(conn, query, query_type)

        return {
            "statusCode": 200,
            "body": json.dumps({"success": True}),
            "headers": {"Content-Type": "application/json"},
        }