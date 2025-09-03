import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
from pathlib import Path
from messaging.publish_message import send_mail_exception

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