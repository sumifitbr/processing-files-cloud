# Arquivo special_functions.py
# Ultima Alteração: 2025-08-05 10:00:00
import pandas as pd
from datetime import datetime

def hash_if(cod_oper: str, cod_prod_opel_locl, cod_mod_tx, tx_or_oper):
    """
    English:
    Hashes or modifies the operation code based on product and operation type.
    
    Parameters:
    - cod_oper (str): Operation code.
    - cod_prod_opel_locl: Local product operation code.
    - cod_mod_tx: Transaction mode code.
    - tx_or_oper: Indicates whether to return transaction mode or hashed operation code.
    
    Returns:
    - str: Modified transaction mode or hashed operation code.

    Español:
    Hashea o modifica el código de operación basado en el producto y tipo de operación.
    
    Parámetros:
    - cod_oper (str): Código de operación.
    - cod_prod_opel_locl: Código de operación de producto local.
    - cod_mod_tx: Código de modo de transacción.
    - tx_or_oper: Indica si se debe devolver el modo de transacción o el código de operación hasheado.
    
    Retorna:
    - str: Modo de transacción modificado o código de operación hasheado.
    """
    if (cod_prod_opel_locl == "2") or (cod_prod_opel_locl == "3"):
        if len(cod_oper) == 11:
            cod_mod_tx = (cod_oper)[-2:]
            cod_oper = (cod_oper)[:-2]
        if tx_or_oper == "tx":
            return cod_mod_tx
        else:
            cod_oper = hash(str(int(cod_oper)))
            return cod_oper
    else:
        if tx_or_oper == "tx":
            return ""
        else:
            return cod_oper

def skip_hash_traza_doc(columna: str):
    """
    English:
    Skips hashing for specific document trace values.
    
    Parameters:
    - columna (str): Column value to be checked.
    
    Returns:
    - str: Original or hashed column value.

    Español:
    Omite el hash para valores específicos de trazas de documentos.
    
    Parámetros:
    - columna (str): Valor de la columna a verificar.
    
    Retorna:
    - str: Valor original o hasheado de la columna.
    """
    if columna == "         ":
        return "         "
    elif columna == "000000000":
        return "000000000"
    else:
        return hash(columna)

def skip_hash_vale(columna: str):
    """
    English:
    Skips hashing for specific voucher values.
    
    Parameters:
    - columna (str): Column value to be checked.
    
    Returns:
    - str: Original or hashed column value.

    Español:
    Omite el hash para valores específicos de vales.
    
    Parámetros:
    - columna (str): Valor de la columna a verificar.
    
    Retorna:
    - str: Valor original o hasheado de la columna.
    """
    if columna == "9999999":
        return "9999999"
    elif columna == "999999999999":
        return "999999999999"
    else:
        return hash(columna)

def delete_cero(columna: str):
    """
    English:
    Converts a column value to an integer and removes leading zeros.
    
    Parameters:
    - columna (str): Column value to be processed.
    
    Returns:
    - str: Integer value as a string without leading zeros.

    Español:
    Convierte un valor de columna a entero y elimina ceros a la izquierda.
    
    Parámetros:
    - columna (str): Valor de la columna a procesar.
    
    Retorna:
    - str: Valor entero como cadena sin ceros a la izquierda.
    """
    columna = isInt(columna)
    columna = str(int(columna))
    return columna

def toInt(value):
    """
    English:
    Converts a value to an integer string if possible.
    
    Parameters:
    - value: Value to be converted.
    
    Returns:
    - str: Integer value as a string or original value if conversion fails.

    Español:
    Convierte un valor a cadena de entero si es posible.
    
    Parámetros:
    - value: Valor a convertir.
    
    Retorna:
    - str: Valor entero como cadena o valor original si la conversión falla.
    """
    try:
        int(value)
        return str(value)
    except ValueError:
        return value

def isInt(value):
    """
    English:
    Checks if a value can be converted to an integer.
    
    Parameters:
    - value: Value to be checked.
    
    Returns:
    - str: Original value if convertible, otherwise a default large number.

    Español:
    Verifica si un valor puede ser convertido a entero.
    
    Parámetros:
    - value: Valor a verificar.
    
    Retorna:
    - str: Valor original si es convertible, de lo contrario un número grande por defecto.
    """
    try:
        int(value)
        return value
    except ValueError:
        return "999999999999999999"

def delete_any(columna: str):
    """
    English:
    Removes the last character from a column value.
    
    Parameters:
    - columna (str): Column value to be modified.
    
    Returns:
    - str: Modified column value with the last character removed.

    Español:
    Elimina el último carácter de un valor de columna.
    
    Parámetros:
    - columna (str): Valor de la columna a modificar.
    
    Retorna:
    - str: Valor de la columna modificado con el último carácter eliminado.
    """
    columna = columna[:-1]
    return columna

def concat_fields(data: pd.DataFrame, special_functions_str: str) -> pd.DataFrame:
    """
    Concatena duas colunas de string (column1 e column2) em uma nova coluna com o nome especificado.

    Args:
        data (pd.DataFrame): O DataFrame a ser modificado.
        special_functions_str (str): String no formato 'concat_fields:column1:column2:columns_new_name'

    Returns:
        pd.DataFrame: O DataFrame modificado com a nova coluna e as colunas originais removidas.
    """
    # Extrai os valores da string
    parts = special_functions_str.split(":")
    print(f"Separando valores recebido: {parts}")

    func_name = parts[0]
    print(f"Nome da função a ser processada: {func_name}")

    column1 = parts[1]
    column2 = parts[2]
    column_new_name = parts[3]
    print(f"Argumentos: {column1}, {column2}")
    print(f"Coluna de resultados: {column_new_name}")
    print("----------------------------------------")

    # Verifica se as colunas existem no Dataframe
    if column1 not in data.columns or column2 not in data.columns:
        raise ValueError(f"Colunas '{column1}' ou '{column2}' não encontradas no Dataframe")
    
    # Concatena as colunas e aplica a máscara de formato
    def format_columns(row):
        column1_str = row[column1]
        column2_str = row[column2]
        try:
            # Converte as strings para datetime e aplica o formato desejado
            formatted_date = datetime.strptime(column1_str, "%Y%m%d").strftime("%Y-%m-%d")
            formatted_time = datetime.strptime(column2_str[:6], "%H%M%S").strftime("%H:%M:%S") # String limitada a 6 caracteres para compor a hora:minuto:segundo
            return f"{formatted_date} {formatted_time}"
        except ValueError:
            return f"{column1_str} {column2_str}"

    # Concatena as colunas
    print(f"Nova coluna: {column_new_name}")
    # data[column_new_name] = data[column1].astype(str) + ' ' + data[column2].astype(str)
    data[column_new_name] = data.apply(format_columns, axis=1)
    print(data.head(3))

    # Remove as colunas originais
    #data = data.drop(columns=[column1, column2])

    return data

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
    
def apply_special_functions(data,special_function_str):
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