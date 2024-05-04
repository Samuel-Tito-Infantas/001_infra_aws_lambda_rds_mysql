import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(parent_dir)

from aux_functions.log_function import LoggerJob
from parameters import key_parameters

import boto3
from botocore.exceptions import ClientError
import json

log = LoggerJob().log()


def get_secret(env_var: str) -> tuple:
    """
    Retrieve credentials from AWS Secrets Manager.

    Args:
        set_enviroment (str): A dictionary containing parameters necessary for
        retrieving the secret.

    Returns:
        tuple: A tuple containing the retrieved credentials and RDS proxy host.
            The tuple structure is (user_name, password, rds_proxy_host).

    Raises:
        ClientError: An error occurred when interacting with the AWS Secrets
            Manager service.
        Exception: An unexpected error occurred during the processing of the retrieved
            secret.
    """

    env_variables = key_parameters[env_var]
    secret_name = env_variables["SECRET_NAME"]
    region_name = env_variables["REGION_NAME"]
    rds_proxy_host = env_variables["RDS_PROXY_HOST"]

    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        log.info("Retrieved secret successfully")

    except ClientError as e:
        error_possible_messages(e, secret_name)
        log.error("Error retrieving secret: %s", e)

    else:
        try:
            user_name, password = filter_secret_string(get_secret_value_response)
            return user_name, password, rds_proxy_host
        except Exception as e:
            log.error("Error processing secret: %s", e)
    finally:
        log.info("Completed secret retrieval")


def error_possible_messages(erro_object: object, secret_name: str) -> None:
    """
    Handle potential errors when interacting with AWS Secrets Manager.

    Args:
        error_object: An exception object representing the error encountered during the
        interaction.
        secret_name (str): The name of the secret associated with the error, if
        applicable.

    Return:
        None:
         This function does not raise any exceptions itself but handles them
        internally.
    """

    if erro_object.response["Error"]["Code"] == "ResourceNotFoundException":
        log.error("The requested secret " + secret_name + " was not found")
    elif erro_object.response["Error"]["Code"] == "InvalidRequestException":
        log.error("The request was invalid due to:", erro_object)
    elif erro_object.response["Error"]["Code"] == "InvalidParameterException":
        log.error("The request had invalid params:", erro_object)
    elif erro_object.response["Error"]["Code"] == "DecryptionFailure":
        log.error(
            "The requested secret can't be decrypted using the provided KMS key:",
            erro_object,
        )
    elif erro_object.response["Error"]["Code"] == "InternalServiceError":
        log.error("An error occurred on service side:", erro_object)
    else:
        log.error(f"Error_message: {erro_object.response}")


def filter_secret_string(get_secret_value_response: str) -> tuple:
    """
    Filter and extract username and password from the secret value response.

    Args:
        get_secret_value_response (str): A dictionary containing the secret value
            response returned by AWS Secrets Manager.
    Returns:
        tuple: A tuple containing the extracted username and password of Data Base.

    Raises:
        KeyError: If the 'SecretString' or 'SecretBinary' field is not found in
            the get_secret_value_response dictionary.
    """

    if "SecretString" in get_secret_value_response:
        text_secret_data = get_secret_value_response["SecretString"]
        text_secret_data = json.loads(str(text_secret_data))
        return text_secret_data["username"], text_secret_data["password"]

    else:
        binary_secret_data = get_secret_value_response["SecretBinary"]
        log.warning(
            "Secret is in binary format, consider handling binary data appropriately"
        )
        raise
