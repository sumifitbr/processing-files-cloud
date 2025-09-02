# Versao 5: 2025-05-16 17:00:00
#### CODIDO PROCESSAMENTO #######
import os
import json
import boto3
import pandas as pd
import numpy as np
from datetime import datetime
from io import StringIO
import sys
from pathlib import Path
import re
from botocore.exceptions import ClientError
import base64
import csv
import hashlib
import subprocess
import argparse
from special_functions import *

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

# Função hash subprocess
def hash_deterministic(valor: str):
    #print("=== Entrada ===")
    #print(f"O dado de entrada: {valor}")
    resultado = hash(valor)
    #print("=== Saida ===")
    #print(f"O dado de saida: {resultado}")
    return resultado

# Função que executa hash de colunas
def apply_hash(data, columns_str):
    columns = [col.strip() for col in columns_str.split(',')]
    # Aplica o hash nas colunas especificadas
    for coluna in columns:
        if coluna in data.columns:
            data[coluna] = data[coluna].apply(lambda x: hash_deterministic(x))    
    return data

# Função que executa a eliminação de colunas
def drop_columns(data, columns):
    return data.drop(columns=[col for col in columns if col in data.columns])

# Função que executa a renomeação de colunas
def rename_columns(data, columns_mapping):
    return data.rename(columns=columns_mapping)

# Função que executa a adição de colunas
def add_columns(data, add_columns_string):
    """
    Adiciona colunas ao DataFrame com base em uma string no formato "coluna1:valor1,coluna2:valor2".
    """
    #columns = dict(pair.split(":") for pair in add_columns_string.split(","))
    columns = {}
    for pair in add_columns_string.split(","):
        col, default = pair.split(":") if ":" in pair else (pair, "")
        columns[col] = default

    for col, default in columns.items():
        data[col] = default
    return data

# Função para formatar a data
def dateFormat(df, date_format_params):
    """
    Converte o formato de data para as colunas especificadas.
    Args:
        df: DataFrame pandas
        date_format_params: String no formato "coluna:formato_entrada:formato_saida[,coluna:formato_entrada:formato_saida]"
    Returns:
        DataFrame com as datas convertidas
    """
    try:
        # Separa múltiplas colunas se houver
        columns_to_process = date_format_params.split(',')

        for column_config in columns_to_process:
            # Remove espaços em branco e separa os parâmetros
            column_name, input_format, output_format = [x.strip() for x in column_config.split(':')]

            print(f"--> Convertendo formato de data para coluna: {column_name}")
            print(f"    Formato de entrada: {input_format}")
            print(f"    Formato de saída: {output_format}")

            # Verifica se a coluna existe no DataFrame
            if column_name not in df.columns:
                print(f"    Aviso: Coluna {column_name} não encontrada no DataFrame")
                continue

            # Cria uma nova lista para armazenar as datas convertidas
            converted_dates = []

            # Processa cada valor na coluna
            for date_value in df[column_name]:
                try:
                    if pd.isna(date_value) or str(date_value).strip() == '':
                        converted_dates.append('')
                    else:
                        # Tenta converter a data usando o formato especificado
                        parsed_date = datetime.strptime(str(date_value).strip(), input_format)
                        converted_dates.append(parsed_date.strftime(output_format))
                except Exception as e:
                    print(f"    Aviso: Erro ao converter valor '{date_value}' na coluna {column_name}: {str(e)}")
                    converted_dates.append('')

            # Substitui a coluna original com os valores convertidos
            df[column_name] = converted_dates

            print(f"    Conversão concluída para coluna: {column_name}")

        return df

    except Exception as e:
        error_message = f"Erro ao processar conversão de datas: {str(e)} - Verifique no arquivo de parametro, o formato cadastro é: {date_format_params}"
        print(error_message)
        send_mail_exception(
            file_name="dateFormat",
            process_name="dateFormat",
            error_type=type(e).__name__,
            additional_info=error_message
        )
        raise

# Função para contar linhas em um arquivo no S3
def count_lines_in_s3_file(bucket, file_key):
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=file_key)

        # Lê o conteúdo do arquivo em chunks para evitar problemas com arquivos grandes
        chunk_size = 1024 * 1024  # 1MB por chunk
        line_count = 0

        # Inicializa o buffer para conteúdo residual entre chunks
        remainder = ""

        # Processa o arquivo em chunks
        for chunk in response['Body'].iter_chunks(chunk_size=chunk_size):
            # Tenta descobrir em UTF-8, Windows-1252 ou Latin-1
            try:
                content = chunk.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    content = chunk.decode('windows-1252')
                except UnicodeDecodeError:
                    content = chunk.decode('ISO-8859-1', errors='replace')
            
            content = remainder + content
            lines = content.split('\n')

            # Guarda a última linha parcial para o próximo chunk
            remainder = lines[-1]

            # Conta as linhas completas neste chunk
            line_count += len(lines) - 1

        # Adiciona a última linha se não estiver vazia
        if remainder:
            line_count += 1

        print(f"Total de linhas no arquivo {file_key}: {line_count}")
        return line_count

    except Exception as e:
        error_message = f"Erro ao contar linhas no arquivo {file_key} no bucket {bucket}: {e}"
        print(error_message)
        send_mail_exception(
            file_name=file_key,
            process_name="count_lines_in_s3_file",
            error_type=type(e).__name__,
            additional_info=error_message
        )
        return None

# Função para recuperar corretamente a fecha_ref de cada arquivo
def extract_fecha_ref(str_arquivo):
    # Captura todas as sequências de 8 dígitos
    file_findall = re.findall(r'\d{8}', str_arquivo)

    # Filtramos apenas os blocos de 8 dígitos que NÃO fazem parte de sequências maiores
    validadet_fecha_ref = [data for data in file_findall if re.search(rf'\D{data}\D', f"_{str_arquivo}_")]

    if validadet_fecha_ref:
        # Pegamos a última data válida (mais próxima do final do nome do arquivo)
        fecha_ref = validadet_fecha_ref[-1]
        return datetime.strptime(fecha_ref, '%Y%m%d').strftime('%Y-%m-%d')
    else:
        # Se não encontrar uma data válida, retorna a data atual
        return datetime.now().strftime('%Y%m%d')
        
# Função para salvar o arquivo processado no S3
def save_to_s3_transient_zone(bucket_name, key, data):
    s3 = boto3.client("s3")
    try:
        buffer = StringIO()
        data.to_csv(buffer, index=False, sep=";")
        buffer.seek(0)
        s3.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
        print(f"Arquivo salvo com sucesso no S3: s3://{bucket_name}/{key}")
    except Exception as e:
        error_message = f"Erro ao salvar arquivo no S3: {e}"
        print(error_message)
        send_mail_exception(
            file_name=key,
            process_name="save_to_s3_transient_zone",
            error_type=type(e).__name__,
            additional_info=error_message
        )

# Função que renomeia arquivos de saida pelo REGEX
def defined_filename_output(nome_arquivo, regex_padroes):
    """
    Define o nome do arquivo de saída com base no regex e no filename_output.
    Args:
        nome_arquivo (str): Nome do arquivo de entrada.
        regex_padroes (list): Lista de tuplas com regex e filename_output.
    Returns:
        str: Nome do arquivo de saída ou mensagem de erro caso não corresponda.
    """

    # Remove caminho e extensão, normaliza espaços
    base = os.path.splitext(os.path.basename(nome_arquivo.strip()))[0]
    print(f"Arquivo base: {base}")

    for regex, filename_output in regex_padroes:
        print(f"Meu regex: {regex}")
        print(f"Meu Arquivo de saida: {filename_output}")
        if re.fullmatch(regex, base):
            # preserve o nome inteiro
            if filename_output == "{match}":
                return f"{base.lower()}"
            # adiciona sufixo de data
            data_match = re.search(r"\d{8}", base)
            if data_match:
                return f"{filename_output}{data_match.group(0)}".lower()
            else:
                # fallback caso não encontre a data
                return f"{filename_output}".lower()

    return f"Regex {filename_output} contém problemas"

# Função para mover arquivo no S3
def move_to_backup(bucket_name, source_key, backup_key):
    s3 = boto3.client("s3")
    try:
        s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': source_key}, Key=backup_key)
        print(f"Arquivo movido para backup: s3://{bucket_name}/{backup_key}")
        s3.delete_object(Bucket=bucket_name, Key=source_key)
        print(f"Arquivo original removido: s3://{bucket_name}/{source_key}")
    except Exception as e:
        error_message = f"Erro ao mover arquivo para backup: {e}"
        print(error_message)
        send_mail_exception(
            file_name=source_key,
            process_name="move_to_backup",
            error_type=type(e).__name__,
            additional_info=error_message
        )

# Função para limpar diretório tracking no S3
def clear_s3_directory(bucket, prefix):
    try:
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in response.get('Contents', []):
            s3.delete_object(Bucket=bucket, Key=obj['Key'])
        print(f"# Limpeza completa do diretório: s3://{bucket}/{prefix}...OK")
    except Exception as e:
        error_message = f"Erro ao limpar diretório S3: {e}"
        print(error_message)
        send_mail_exception(
            file_name=prefix,
            process_name="clear_s3_directory",
            error_type=type(e).__name__,
            additional_info=error_message
        )

# Função auxiliar para salvar estatísticas iniciais
def save_statistics_initial(bucket_name, path_local, filename_path_local, extension_file_source, validated_files_source, total_lines, file_creation_date, file_creation_time, modification_date, modification_time, path_s3, folder_s3, processing_start, creation_date_full, partition_date, fecha_ref, table_name):
    
    s3 = boto3.client("s3")
    prefix = 'tracking/tracking_start.csv'

    try:
        s3.delete_object(Bucket=bucket_name, Key=prefix)
        print(f"# Limpeza do arquivo: s3://{bucket_name}/{prefix}...OK")
    except Exception as e:
        error_message = f"Erro ao limpar o arquivo s3://{bucket_name}/{prefix}: {e}"
        print(error_message)
        send_mail_exception(
            file_name=prefix,
            process_name="clear_file_statistics_initial",
            error_type=type(e).__name__,
            additional_info=error_message
        )    
    
    stats_csv_path = prefix

    # Criar novo registro de estatísticas
    #table_name = f"tbl_{path_local.split('/')[-1]}"
    print(f"Tabela a ser carregada: {table_name}")
    new_stats = {
        'path_local': path_local,
        'filename_path_local': filename_path_local,
        'extension_file_source': extension_file_source,
        'validated_files_source': validated_files_source,
        'total_lines': total_lines,
        'file_creation_date': file_creation_date,
        'file_creation_time': file_creation_time,
        'modification_date': modification_date,
        'modification_time': modification_time,
        'path_s3': bucket_name,
        'folder_s3': folder_s3,
        'processing_start': processing_start,
        'creation_date_full': creation_date_full,
        'partition_date': partition_date,
        'fecha_ref': fecha_ref,
        'table_name': table_name
    }

    try:
        # Tentar ler arquivo existente
        try:
            response = s3.get_object(Bucket=bucket_name, Key=stats_csv_path)
            existing_stats = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')), sep=";")
        except:
            # Se arquivo não existir, criar DataFrame vazio
            existing_stats = pd.DataFrame()

        # Adicionar nova linha às estatísticas existentes
        new_stats_df = pd.DataFrame([new_stats])
        stats_df = pd.concat([existing_stats, new_stats_df], ignore_index=True)

        # Salvar estatísticas atualizadas no S3
        buffer = StringIO()
        stats_df.to_csv(buffer, sep=";", index=False)
        buffer.seek(0)
        s3.put_object(Bucket=bucket_name, Key=stats_csv_path, Body=buffer.getvalue())
        print(f"Estatísticas atualizadas no S3: s3://{bucket_name}/{stats_csv_path}")
    except Exception as e:
        error_message = f"Erro ao salvar estatísticas iniciais no S3: {e}"
        print(error_message)
        send_mail_exception(
            file_name=stats_csv_path,
            process_name="save_statistics_initial",
            error_type=type(e).__name__,
            additional_info=error_message
        )

# Função auxiliar para salvar estatísticas finais
def save_statistics_final(bucket_name, path_local, filename_path_local, filename_s3, extension_file_target, processing_end, processing_start):
    s3 = boto3.client("s3")
    stats_csv_path = 'tracking/tracking_end.csv'
    
    try:
        s3.delete_object(Bucket=bucket_name, Key=stats_csv_path)
        print(f"# Limpeza do arquivo: s3://{bucket_name}/{stats_csv_path}...OK")
    except Exception as e:
        error_message = f"Erro ao limpar o arquivo s3://{bucket_name}/{stats_csv_path}: {e}"
        print(error_message)
        send_mail_exception(
            file_name=stats_csv_path,
            process_name="clear_file_statistics_final",
            error_type=type(e).__name__,
            additional_info=error_message
        )     
    
    # Convert timestamps to datetime objects
    start_datetime = datetime.strptime(processing_start, '%Y-%m-%d %H:%M:%S')
    end_datetime = datetime.strptime(processing_end, '%Y-%m-%d %H:%M:%S')
    print(f"start_datetime execution: {start_datetime}")
    print(f"end_datetime execution: {end_datetime}")

    # Calculate the time difference
    time_difference = end_datetime - start_datetime # Atraves do campo processing_start e processing_end é gerado o tempo de execução
    print(f"Tempo gasto pelo processo: {str(time_difference)}")

    # Format the time difference as hh:mm:ss
    time_execution = str(time_difference)    

    # Criar novo registro de estatísticas
    new_stats = {
        'path_local': path_local,
        'filename_path_local': filename_path_local,
        'filename_s3': filename_s3,
        'extension_file_target': extension_file_target,
        'processing_end': processing_end,
        'status':'PROCESSADO',
        'time_execution': time_execution # Formato : "00:00:00" 
    }

    try:
        # Tentar ler arquivo existente
        try:
            response = s3.get_object(Bucket=bucket_name, Key=stats_csv_path)
            existing_stats = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')), sep=";")
        except:
            # Se arquivo não existir, criar DataFrame vazio
            existing_stats = pd.DataFrame()

        # Adicionar nova linha às estatísticas existentes
        new_stats_df = pd.DataFrame([new_stats])
        stats_df = pd.concat([existing_stats, new_stats_df], ignore_index=True)

        # Salvar estatísticas atualizadas no S3
        buffer = StringIO()
        stats_df.to_csv(buffer, sep=";", index=False)
        buffer.seek(0)
        s3.put_object(Bucket=bucket_name, Key=stats_csv_path, Body=buffer.getvalue())
        print(f"Estatísticas atualizadas no S3: s3://{bucket_name}/{stats_csv_path}")
    except Exception as e:
        error_message = f"Erro ao salvar estatísticas finais no S3: {e}"
        print(error_message)
        send_mail_exception(
            file_name=stats_csv_path,
            process_name="save_statistics_final",
            error_type=type(e).__name__,
            additional_info=error_message
        )

# Função para gerar o arquivo de resultados de estatísticas dos processos
def generate_tracking_results(bucket_name):
    """
    Realiza o join dos arquivos tracking_start.csv e tracking_end.csv e
    gera o arquivo tracking_results.csv particionado por data, fazendo append se já existir.
    """
    try:
        s3_client = boto3.client('s3')

        # Obtém a data atual para particionamento
        current_date = datetime.now()
        partition_path = current_date.strftime('airflow_envios/year=%Y/month=%m/day=%d')
        output_key = f"{partition_path}/tracking_results.csv"

        try:
            # Lê tracking_start.csv
            response_start = s3_client.get_object(Bucket=bucket_name, Key='tracking/tracking_start.csv')
            df_start = pd.read_csv(StringIO(response_start['Body'].read().decode('utf-8')), sep=';')

            # Lê tracking_end.csv
            response_end = s3_client.get_object(Bucket=bucket_name, Key='tracking/tracking_end.csv')
            df_end = pd.read_csv(StringIO(response_end['Body'].read().decode('utf-8')), sep=';')

            # Prepara a nova coluna 'filename_s3' no df_start para compor o JOIN
            df_start['filename_s3'] = df_start['folder_s3'].apply(lambda x: x.split('/')[-1] if isinstance(x, str) else x)

            # Realiza o join dos dataframes
            df_results = pd.merge(
                df_start,
                df_end,
                on=['path_local', 'filename_path_local', 'filename_s3'],
                how='inner'
            )

            # Adiciona coluna table_name se não existir
            if 'table_name' not in df_results.columns:
                df_results['table_name'] = ''

            # Reorganiza as colunas na ordem especificada
            columns_order = [
                'table_name', 'path_local', 'filename_path_local', 'extension_file_source', 
                'validated_files_source', 'total_lines','file_creation_date', 'file_creation_time', 'modification_date', 'modification_time', 
                'path_s3', 'folder_s3', 'filename_s3', 'extension_file_target', 'processing_start', 
                'processing_end', 'time_execution', 'status', 'partition_date', 'fecha_ref'
            ]

            df_results = df_results.reindex(columns=columns_order).fillna('')

            # Verifica se o arquivo já existe no S3
            try:
                response_existing = s3_client.get_object(Bucket=bucket_name, Key=output_key)
                df_existing = pd.read_csv(StringIO(response_existing['Body'].read().decode('utf-8')), sep=';')

                # Concatena evitando duplicidades
                df_final = pd.concat([df_existing, df_results]).drop_duplicates()
            except s3_client.exceptions.NoSuchKey:
                df_final = df_results  # Se não existir, apenas usa o novo dataframe

            # Garante que todas colunas sejam strings antes de salvar
            df_final = df_final.astype(str)

            # Salva o dataframe atualizado
            buffer = StringIO()
            df_final.to_csv(buffer, sep=';', index=False)
            buffer.seek(0)

            s3_client.put_object(Bucket=bucket_name, Key=output_key, Body=buffer.getvalue())
            print(f"Arquivo atualizado com sucesso: s3://{bucket_name}/{output_key}")

        except Exception as e:
            raise Exception(f"Erro ao ler arquivos de tracking: {str(e)}")

    except Exception as e:
        error_message = f"Erro ao gerar arquivo de resultados: {str(e)}"
        print(error_message)
        send_mail_exception(
            file_name='tracking_results.csv',
            process_name="generate_tracking_results",
            error_type=type(e).__name__,
            additional_info=error_message
        )
        
# Função para envio de mensagens via SNS
def publish_message_to_sns(subject, message):
    """
    Publica uma mensagem no tópico SNS.
    """
    try:
        sns_client = boto3.client("sns", region_name=AWS_REGION)
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message,
        )
        print(f"Mensagem enviada com sucesso! MessageId: {response['MessageId']}")
    except ClientError as e:
        print(f"Erro ao enviar mensagem SNS: {e}")
        raise

# Função para criar mensagens de erro para envio SNS
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
    publish_message_to_sns(subject=f"Erro no processo {process_name}", message=message_email)

# Função que retira espaços em string do dataframe
def clean_column(col):
    """
    Remove espaços e aspas de colunas de texto
    Converte valores nulos em string vazia
    """
    if col.dtype == object or col.dtype.name == "string":
        return col.apply(lambda x: str(x).strip().replace('"','').replace("'",'') if pd.notnull(x) else "")
    return col

# Função SPECIAL FUNCTIONS
# Essa função irá receber nomes e parametros do campo special_functions no JSON PARAMETERS
# Quando o parametro special_functions for <> de NULL o que estiver configurado será executado
# na def_process_file_generic
#
# Exemplo para cadastro no JSON PARAMETERS
# "special_functions":"somavalores:col1:col2:col_soma,multiplica:col3:col4:col_prod"
# Onde:
#   somavalores --> é o nome da função
#   col1 --> primeiro argumento da função
#   col2 --> segundo argumento da função
#   col_soma --> é a nova coluna que vai receber o calculo da função em special_functions.py e que irá ser gravada no dataframe de saida com o resultado
def apply_special_functions(data,special_function_str):
    if special_function_str in ["NULL",""]:
        print('--> Nenhuma função especial aplicada.')
        return data
    
    functions = [func.strip() for func in special_function_str.split(",") if func.strip()] # separador de funções sempre virgula

    for func_def in functions:
        print(f"Valor da func_def: {func_def}")
        parts = func_def.split(":")
        func_name = parts[0]
        results_column = parts[-1]
        #func_name = "concat_fields"
        #results_column = "fecha_hora"
        print("---------------------------------------------")

        # Busca dinamicamente a função definida no codigo
        func = globals().get(func_name)
        if not callable(func):
            print(f"--> Função '{func_name}' não definida no codigo...ignorando.")
            continue

        try:
            # Aplica a função e adiciona o resultado com uma nova coluna
            data.apply(lambda row: func(data, func_def), axis=1)
            print(f"--> Função: '{func_name}' aplicada com sucesso com os argumentos: {func_def}, o resultado na coluna: {results_column}")
        except Exception as e:
            print({str(e)})
            print(f"Erro ao aplicar a função especial '{func_name}' e argumentos {func_def}: {str(e)}")
            send_mail_exception(
                file_name=file_path,
                process_name=f"Error: Special Function: {func_name}. Argumentos utilizados: {func_def}",
                error_type=type(e).__name__,
                additional_info=str(e)
            )
            raise            
    
    return data

# Função principal para processamento genérico
def process_file_generic(parameters, bucket_name, path_local_landing_zone, table_name):
    try:
        # captura data e hora inicio processamento
        processing_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")        
        # Extraindo os parâmetros necessários para a execução
        param_key = parameters.get("name", "")
        file_path = parameters.get("specific_file", "")  # Novo parâmetro para arquivo específico
        print("# Key name (DYNAMODB) a ser processada: " + param_key)

        # Parâmetros fixos
        path_to_table = f"s3://{bucket_name}/landing-zone/{path_local_landing_zone}/"
        print(f"# Diretório S3: {path_to_table}")

        # Limpeza dos arquivos de TRACKING - s3://<bucket_name>/tracking/
        clear_s3_directory(bucket_name, "tracking/")

        # Processar apenas o arquivo específico
        str_arquivo = file_path.split("/")[-1]
        print(f"# Arquivo a ser processado: {str_arquivo}")

        # Caminho completo do arquivo no S3
        path_file_full = f"s3://{bucket_name}/{file_path}"
        print(f"# Caminho completo: {path_file_full}")

        # Verificando extensão de arquivo
        extension_file = parameters.get("extension_file", "CSV")

        # Coletando valores de separator_file_read para determinar se o processamento é 
        # Se separator_file_read = ';' então Delimitado
        # Se separator_file_read = 'NULL' então Posicional
        separator_file_read = parameters.get("separator_file_read")

        # Coletando valores de widths para arquivo posicionais
        widths_param = parameters.get("widths", None)

        if not widths_param or widths_param.strip() == "" or widths_param == "NULL":
            widths_param = None

        print(f"# Tipo de processamento: {'Arquivo Posicional' if widths_param is not None else 'Arquivo Delimitado'}")

        # Inicializa a variável widths_str
        widths_str = None

        # Montando as colunas existentes no cadastro do Dynamodb (columns_names)
        columns_names = parameters.get("column_names", "")
        print(f"# Colunas: {columns_names}")
        # Converte a string em lista apenas se não estiver vazia
        columns_list = columns_names.split(",") if columns_names else None
        # Determina o número correto de colunas baseado no header ou na lista de colunas
        skip_rows = int(parameters.get("skip_rows", 0))
        print(f"SkipRows utilizado: {skip_rows}")

        # Função que validar a quantidade de colunas de cada arquivo
        def get_number_of_columns(file_path, is_fixed_width=False):
            try:
                if skip_rows == 0:
                    if is_fixed_width:
                        # Para arquivos de largura fixa
                        widths_str = json.loads(widths_param)
                        return len(widths_str)
                    else:
                        # Para arquivos delimitados
                        header_df = pd.read_csv(
                            file_path,
                            sep=parameters.get("separator_file_read", ";"),
                            encoding=parameters.get("encoding_file_read", "utf-8"),
                            nrows=1,
                            dtype=str
                        )
                        return len(header_df.columns)
                else:
                    return len(columns_list) if columns_list else 0
            except Exception as e:
                error_message = f"Erro ao determinar número de colunas: {str(e)}"
                print(error_message)

                # Enviando notificação de erro via SNS
                send_mail_exception(
                    file_name=file_path,
                    process_name="get_number_of_columns",
                    error_type=type(e).__name__,
                    additional_info=error_message
                )
                return 0

        # Verifica o tipo de arquivo a ser processado (Delimitado ou Posicional)
        #if extension_file in {"DAT", "TXT", "dat", "txt"} and widths_param is not None:
        if extension_file.lower() in {"dat", "txt"} and separator_file_read == "NULL":            
            print("##################################################################")
            print("Processando arquivos posicionais...")
            n_cols = get_number_of_columns(path_file_full, is_fixed_width=True)
            print(f"Numero de colunas: {n_cols}")
            widths_str = json.loads(widths_param)
            print(widths_str)

            data = pd.read_fwf(
                path_file_full,
                widths=widths_str[:n_cols],  # Usa apenas o número correto de colunas
                encoding=parameters.get("encoding_file_read", "utf-8"),
                skiprows=skip_rows,
                #skiprows=skiprows_param,
                names=columns_list if skip_rows > 0 else None,
                #names=names_param,
                dtype=str
            )
            print(data)

        elif extension_file.lower() in {"csv", "txt", "lis", "dat"}:
            print("Processando arquivos delimitados...")
            n_cols = get_number_of_columns(path_file_full, is_fixed_width=False)
            print(f"Numero de colunas: {n_cols}")

            data = pd.read_csv(
                path_file_full,
                sep=parameters.get("separator_file_read", ";"),
                encoding=parameters.get("encoding_file_read", "utf-8"),
                dtype=str,
                skiprows=skip_rows,
                header=None if skip_rows > 0 else 0,
                names=columns_list if skip_rows > 0 else None,
                # skiprows=skiprows_param
                # header=header_param
                # names=names_param
                index_col=None,
                usecols=range(n_cols),  # Usa apenas o número correto de colunas
                storage_options={'anon': False}
            )

        # Verificação de consistência do número de colunas
        if data.shape[1] != n_cols:
            print(f"Aviso: O arquivo tem {data.shape[1]} colunas, mas esperávamos {n_cols} colunas.")
            if data.shape[1] > n_cols:
                print("Removendo colunas extras...")
                data = data.iloc[:, :n_cols]

        if data.empty:
            print(f"# Arquivo {str_arquivo} contém apenas o cabeçalho. Pulando processamento.")
            # Enviar o arquivo original para a subpasta LANDING-RESP-TEMP
            move_to_backup(bucket_name, file_path, f"landing-resp-temp/{path_local_landing_zone}/{str_arquivo}")
            return

        # retira espaços das colunas
        data.columns = data.columns.str.strip()
        
        # Função que executa a função clean_column para retirar aspas e NAN ou nan de strings
        data = data.apply(clean_column)

        # Salvar as estatísticas de processamento iniciais
        current_time = datetime.now()

        # Obtém informações do arquivo no S3
        s3_client = boto3.client('s3')
        try:
            file_info = s3_client.head_object(Bucket=bucket_name, Key=file_path)
            file_creation_date = file_info['LastModified'].strftime('%Y-%m-%d')
            file_creation_time = file_info['LastModified'].strftime('%H:%M:%S')
            modification_date = file_info['LastModified'].strftime('%Y-%m-%d')
            modification_time = file_info['LastModified'].strftime('%H:%M:%S')
        except Exception as e:
            print(f"Erro ao obter informações do arquivo: {e}")
            file_creation_date = current_time.strftime('%Y-%m-%d')
            file_creation_time = current_time.strftime('%H:%M:%S')
            modification_date = current_time.strftime('%Y-%m-%d')
            modification_time = current_time.strftime('%H:%M:%S')

        # Extrai a data de referência do nome do arquivo (se disponível)
        fecha_ref = extract_fecha_ref(str_arquivo)
        print(f"O arquivo {str_arquivo} tem a seguinte Fecha Ref: {extract_fecha_ref(str_arquivo)}")

        # Prepara os dados para as estatísticas
        #processing_start =  current_time.strftime("%Y-%m-%d %H:%M:%S")
        stats_data = {
            'bucket_name': bucket_name,
            'path_local': '/'.join(file_path.split('/')[:-1]),  # Caminho sem o nome do arquivo
            'filename_path_local': str_arquivo.split('/')[-1].split('.')[0],  # Nome do arquivo sem extensão
            'extension_file_source': str_arquivo.split('.')[-1].lower(),  # Extensão do arquivo
            'validated_files_source': 'Valido',  # Status padrão do arquivo
            'total_lines': str(count_lines_in_s3_file(bucket_name, file_path)),
            'file_creation_date': file_creation_date,
            'file_creation_time': file_creation_time,
            'modification_date': modification_date,
            'modification_time': modification_time,
            'path_s3': '/'.join(file_path.split('/')[:-1]),  # Caminho S3 sem o nome do arquivo
            'folder_s3': file_path,  # Caminho completo do arquivo
            'processing_start': processing_start,
            'creation_date_full': f"{file_creation_date} {file_creation_time}",
            'partition_date': fecha_ref,
            'fecha_ref': fecha_ref,
            'table_name': table_name
        }

        # Salva estatisticas Iniciais
        save_statistics_initial(
            bucket_name=stats_data['bucket_name'],
            path_local=stats_data['path_local'],
            filename_path_local=f"s3://{bucket_name}/{stats_data['path_local']}",
            #filename_path_local=stats_data['filename_path_local'],
            extension_file_source=stats_data['extension_file_source'],
            validated_files_source=stats_data['validated_files_source'],
            total_lines=stats_data['total_lines'],
            file_creation_date=stats_data['file_creation_date'],
            file_creation_time=stats_data['file_creation_time'],
            modification_date=stats_data['modification_date'],
            modification_time=stats_data['modification_time'],
            path_s3=stats_data['path_s3'],
            folder_s3=stats_data['folder_s3'],
            processing_start=stats_data['processing_start'],
            creation_date_full=stats_data['creation_date_full'],
            partition_date=stats_data['partition_date'],
            fecha_ref=stats_data['fecha_ref'],
            table_name=stats_data['table_name']
        )

        # Aplicando funções operacionais
        print('### Iniciando configuração das funções operacionais ###')
        pd.set_option('display.max_columns', None)

        # Adiciona coluna(s)
        if "add_columns" in parameters and parameters["add_columns"] and parameters["add_columns"] !="NULL":
            print("--> Aplicando Add Columns...OK")
            data = add_columns(data, parameters["add_columns"])

        # Renommeia coluna(s)
        if "rename_columns" in parameters and parameters["rename_columns"] and parameters["rename_columns"] !="NULL":
            print("--> Aplicando Rename Columns...OK")
            rename_mapping = dict(pair.split(":") for pair in parameters["rename_columns"].split(","))
            data = rename_columns(data, rename_mapping)

        # Apaga coluna(s)
        if "drop_columns" in parameters and parameters["drop_columns"] and parameters["drop_columns"] !="NULL":
            print("--> Aplicando Drop Columns...OK")
            drop_columns_list = parameters["drop_columns"].split(",")
            data = drop_columns(data, drop_columns_list)

        # Aplica formatação de DATAS em scoluna(s)
        if "date_format" in parameters and parameters["date_format"] and parameters["date_format"] != "NULL":
            print("--> Aplicando Date Format...OK")
            data = dateFormat(data, parameters["date_format"])
            print("--> Date Format aplicado com sucesso.")

        # Aplica o hash, caso seja necessário
        if "hash_columns" in parameters and parameters["hash_columns"] and parameters["hash_columns"] !="NULL":
            print(f"--> Aplicando Hash Columns...OK")
            data = apply_hash(data, parameters["hash_columns"])

        # Aplica o special_functions, caso seja necessário
        special_functions = parameters["special_functions"]
        print(f"Função a ser executada: {special_functions}")
   
        if "special_functions" in parameters and parameters["special_functions"] and parameters["special_functions"] !="NULL":
            print(f"--> Aplicando funções especiais...OK")
            data = apply_special_functions(data, parameters["special_functions"])            

        # Aplica o delete last row baseando-se na quantidade de linhas do parametro
        delete_last_row = int(parameters.get("delete_last_row", 0))
        if delete_last_row > 0:
            print(f"--> Removendo as últimas {delete_last_row} linha(s) do arquivo...OK")
            data = data.iloc[:-delete_last_row]
            print(f"--> {delete_last_row} linha(s) removida(s) com sucesso.")
        else:
            print("--> Nenhuma linha será removida do final do arquivo.")

        print('### Finalizando configuração das funções operacionais ###')

        # Salvar o arquivo processado no S3
        regex_pattern = parameters.get("regex_pattern", "NULL")
        filename_output = parameters.get("filename_output", "NULL")
        path_s3 = parameters.get("path_s3", "PATH_ERROR")
        load_regex_filename_ouput = [
                {
                    "regex_pattern": regex_pattern,
                    "filename_output": filename_output
                }
            ]
        data_tuples = [(item["regex_pattern"], item["filename_output"]) for item in load_regex_filename_ouput]
        print(f"As tuplas são: {data_tuples}")
        extension_file_target = parameters.get("extension_file_target", "csv")
        nome_saida = defined_filename_output(str_arquivo, data_tuples)
        print(f"# Nome arquivo de entrada: {str_arquivo}")
        print(f"# Nome do arquivo de saída: {nome_saida}.{extension_file_target.lower()}\n")

        # Verifica se o arquivos de saida está com nomenclatura correta (errado: Regex_{match}_contem_problemas_csv)
        if nome_saida.startswith("Regex"):
            print("ATENÇÃO: Favor verificar o REGEX no DYNAMODB")
            # Enviar o arquivo original para a subpasta LANDING-RESP-TEMP
            move_to_backup(bucket_name, file_path, f"landing-resp-temp/{path_local_landing_zone}/{str_arquivo}")
        else:
            filename_s3 = f"transient-zone/{path_s3}/{nome_saida}.{extension_file_target.lower()}"
            save_to_s3_transient_zone(bucket_name, filename_s3, data)
            # Movendo arquivo para backup após processamento
            move_to_backup(bucket_name, file_path, f"landing-zone-archive/{path_local_landing_zone}/{str_arquivo}")

        # Salvar as estatísticas de processamento finais
        path_local_target = f"landing-zone/{path_local_landing_zone}"
        filename_path_local = path_file_full.split("/")
        filename_path_local.pop()
        filename_path_local_target = '/'.join(filename_path_local)
        filename_s3_target = path_file_full.split("/")[-1]
        processing_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_statistics_final(bucket_name, path_local_target, filename_path_local_target, filename_s3_target, extension_file_target, str(processing_end), str(processing_start))

        # Após salvar as estatísticas finais, gerar o arquivo de resultados
        generate_tracking_results(bucket_name)

    except Exception as e:
        error_message = f"Erro ao processar o arquivo {file_path}: {str(e)}"
        print(error_message)

        # Enviando notificação de erro via SNS
        send_mail_exception(
            file_name=file_path,
            process_name="process_file_generic",
            error_type=type(e).__name__,
            additional_info=str(e)
        )
        raise

# Bloco principal
if __name__ == "__main__":
    print("### INICIANDO O PROCESSAMENTO NA ECS TASK###")

    parser = argparse.ArgumentParser(description='Processar parâmetros de entrada.')

    # Adicionar argumentos esperados
    parser.add_argument('--bucket_name', type=str, required=True)
    parser.add_argument('--file_path', type=str, required=True)
    parser.add_argument('--path_local', type=str, required=True)
    parser.add_argument('--table_name', type=str, required=True)

    # Analisar os argumentos
    args = parser.parse_args()

    # Acessar os valores dos argumentos
    bucket_name = args.bucket_name
    file_path = args.file_path
    path_local_landing_zone = args.path_local
    table_name = args.table_name

    # Exibir os valores dos argumentos
    print(f"bucket_name: {bucket_name}")
    print(f"file_path: {file_path}")
    print(f"path_local: {path_local_landing_zone}")
    print(f"table_name: {table_name}")

    # Recupera os valores esperados
    job_run_id = 'EcsJob'
    job_name = 'EcsTask'
    s3_key = f"parameters/latam_parameter_{path_local_landing_zone.replace('/','_').lower()}.json"

    print("#### PARAMETROS RECEBIDOS ####")
    print(f"# JOB RUN ID: {job_run_id}")
    print(f"# JOB NAME: {job_name}")
    print(f"# Nome do bucket: {bucket_name}")
    print(f"# Nome do arquivo: {file_path}")
    print(f"# Path Local: {path_local_landing_zone}")
    print(f"# Table_name: {table_name}")
    print(f"# Arquivo de parametro {s3_key} encontrado em s3://{bucket_name}/{s3_key}")

    try:
        # Carrega o JSON com parâmetros do S3
        parameters = load_json_s3(bucket_name, s3_key)
        if parameters:
            # Adiciona o arquivo específico aos parâmetros
            parameters['specific_file'] = file_path
            process_file_generic(parameters, bucket_name, path_local_landing_zone, table_name)
        else:
            error_message = "Nenhum parâmetro encontrado no arquivo JSON."
            print(error_message)
            send_mail_exception(
                file_name=s3_key,
                process_name="main",
                error_type="ParameterError",
                additional_info=error_message
            )
    except Exception as e:
        error_message = f"Erro na execução principal: {e}"
        print(error_message)
        send_mail_exception(
            file_name=file_path,
            process_name="main",
            error_type=type(e).__name__,
            additional_info=error_message
        )
