import json
import boto3
import base64
from messaging.publish_message import send_mail_exception

# Função para recuperar as configurações do AWS Secrets Manager
def get_secret():
    """
    Função para recuperar as configurações do AWS Secrets Manager
    """
    secret_name = "oca-pipeline-glue"  # Nome do secret no AWS Secrets Manager
    region_name = "us-east-1"  # Região onde o secret está armazenado

    try:
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(decoded_binary_secret)

    except Exception as e:
        error_message = f"Erro ao recuperar secrets: {str(e)} - Verifique as variaveis do aws secret manager oca-pipeline-glu"
        print(error_message)
        send_mail_exception(
            file_name=secret_name,
            process_name="get_secret",
            error_type=type(e).__name__,
            additional_info=error_message
        )
        raise

# Carregar configurações do Secrets Manager
try:
    secrets = get_secret()

    # Configurações da AWS
    AWS_REGION = secrets.get('AWS_REGION', 'us-east-1')
    SNS_TOPIC_ARN = secrets.get('SNS_TOPIC_ARN', '')

except Exception as e:
    print(f"Erro ao configurar variáveis de ambiente: {e}")
    raise