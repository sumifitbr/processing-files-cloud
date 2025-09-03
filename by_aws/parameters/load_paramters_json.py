import json
import boto3
from messaging.publish_message import send_mail_exception


# Função para carregar JSON latam_parameters diretamente do S3
def load_json_s3(bucket_name, s3_key):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        json_content = json.loads(response['Body'].read().decode('utf-8'))

        if isinstance(json_content, list):
            return json_content[0]

        return json_content
    except Exception as e:
        error_message = f"Erro ao carregar arquivo JSON do S3: {e} - Verifique se o arquivo JSON de parametros existe em {bucket_name}/parameters/{s3_key}"
        print(error_message)
        send_mail_exception(
            file_name=s3_key,
            process_name="load_json_s3",
            error_type=type(e).__name__,
            additional_info=error_message
        )
        return None