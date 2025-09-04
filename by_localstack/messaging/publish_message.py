import boto3
from botocore.exceptions import ClientError
from secrets.get_secrets import AWS_REGION, SNS_TOPIC_ARN

AWS_ENDPOINT = "http://localhost:4566"  # LocalStack endpoint


def publish_message_to_sns(subject, message, region_name=AWS_REGION, topic_arn=SNS_TOPIC_ARN):
    """
    Publica uma mensagem no tópico SNS (LocalStack).
    """
    try:
        sns_client = boto3.client(
            "sns",
            region_name=region_name,
            endpoint_url=AWS_ENDPOINT
        )
        response = sns_client.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message,
        )
        print(f"Mensagem enviada com sucesso! MessageId: {response['MessageId']}")
    except ClientError as e:
        print(f"Erro ao enviar mensagem SNS: {e}")
        raise


def send_mail_exception(file_name, process_name, error_type, additional_info):
    """
    Gera e envia a mensagem formatada via SNS para alertar sobre exceptions.
    """
    message_email = f"""
    AWS Glue - Atenção ⚠️

    Olá 👋,

    Um erro ocorreu durante a execução do processo no Glue.

    ⚙️ Processo: {process_name}
    🗃️ Arquivo: {file_name}
    🐞 Tipo de erro: {error_type}
    🔎 Detalhes adicionais: {additional_info}

    Por favor, verifique o log para mais detalhes!
    """
    publish_message_to_sns(
        subject=f"Erro no processo {process_name}",
        message=message_email
    )
