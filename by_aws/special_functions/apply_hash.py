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