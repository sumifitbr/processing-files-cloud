import json
import boto3
import base64
import os
from messaging.publish_message import send_mail_exception

def get_secret():
    """
    Recupera configurações armazenadas no AWS Secrets Manager (LocalStack).
    """
    secret_name = "oca-pipeline-glue"  # Nome do secret
    region_name = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    endpoint_url = os.getenv("AWS_ENDPOINT", "http://localhost:4566")

    try:
        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager",
            region_name=region_name,
            endpoint_url=endpoint_url
        )

        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        if "SecretString" in get_secret_value_response:
            return json.loads(get_secret_value_response["SecretString"])
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response["SecretBinary"])
            return json.loads(decoded_binary_secret)

    except Exception as e:
        error_message = f"Erro ao recuperar secrets: {str(e)}"
        print(error_message)
        send_mail_exception(
            file_name=secret_name,
            process_name="get_secret",
            error_type=type(e).__name__,
            additional_info=error_message
        )
        raise


# Carregar configurações globais ao importar
try:
    secrets = get_secret()
    AWS_REGION = secrets.get("AWS_REGION", "us-east-1")
    SNS_TOPIC_ARN = secrets.get("SNS_TOPIC_ARN", "")
except Exception as e:
    print(f"Erro ao configurar variáveis de ambiente: {e}")
    raise
