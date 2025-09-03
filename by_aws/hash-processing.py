import json
import boto3
import pandas as pd
from datetime import datetime
import argparse
# Import de libs customizadas
from special_functions import *
from messaging.publish_message import send_mail_exception
from operations.operations_type import drop_columns, rename_columns, add_columns, dateFormat, save_to_s3_transient_zone, defined_filename_output, move_to_backup, clear_s3_directory, count_lines_in_s3_file, clean_column
from secrets.get_secrets import *
from statistics.statistics import save_statistics_initial, save_statistics_final, generate_tracking_results
from parameters.load_paramters_json import load_json_s3

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
