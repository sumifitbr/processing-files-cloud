import os
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
from pathlib import Path
import re
from messaging.publish_message import send_mail_exception

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
    
# Função que retira espaços em string do dataframe
def clean_column(col):
    """
    Remove espaços e aspas de colunas de texto
    Converte valores nulos em string vazia
    """
    if col.dtype == object or col.dtype.name == "string":
        return col.apply(lambda x: str(x).strip().replace('"','').replace("'",'') if pd.notnull(x) else "")
    return col

