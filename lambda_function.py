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

        query = f"SELECT * FROM {db_name}.city LIMIT 10"
        # query = """INSERT INTO  decolagem.country
        # (country_id, country_code, country_name, active_status, update_user_id, update_date)
        # VALUES
        # (246, 'NKA', 'Namekusei', False,'f6033e1d-86cf-4120-8c37-934d3c9797bf', CURRENT_TIMESTAMP())"""
        query_type = "read"
        # query_type = "insert"

        df = query_execution_python(conn, query, query_type)

        return {
            "statusCode": 200,
            "body": json.dumps({"success": True}),
            "headers": {"Content-Type": "application/json"},
        }