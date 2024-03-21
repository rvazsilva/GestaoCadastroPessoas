import csv
import psycopg2
from datetime import datetime
import sys, os

# Database connection details
database = "BasesPessoas"
user = "postgres"
password = "postgres"
host = "localhost"
port = "5432"

# File path and update number input
caminho = input("Enter the file path: ")
numero_atualizacao = input("Enter the update number: ")

# CSV file settings
delimiter = ";"
quotechar = '"'
encoding = "ISO 8859-1"

# Log file path
log_file = "/Users/rvazsilva/Temporario/Receita Federal/LogPessoa.txt"


# Dados tabela Estabelecimentos
table_name = 'basespessoas.\"PES_ESTABELECIMENTO\"'
column_names = [
    '\"NUMR_CNPJ_BASICO_EST\"',
    '\"NUMR_CNPJ_ORDEM_EST\"',
    '\"NUMR_CNPJ_DIGITO_EST\"',
    '\"NUMR_CNPJ_COMPLETO_EST\"',
    '\"INDR_MATRIZ_FILIAL_EST\"',
    '\"NOME_FANTASIA_EST\"',
    '\"INDR_SITUACAO_CADASTRAL_EST\"',
    '\"DATA_SITUACAO_CADASTRAL_EST\"',
    '\"CODG_MOTIVO_SITUACAO_CADASTRAL_EST\"',
    '\"NOME_CIDADE_EXTERIOR_EST\"',
    '\"CODG_PAIS_EST\"',
    '\"DATA_INICIO_ATIVIDADE_EST\"',
    '\"DESC_TIPO_LOGRADOURO_EST\"',
    '\"NOME_LOGRADOURO_EST\"',
    '\"NUMR_EDIFICACAO_EST\"',
    '\"DESC_COMPL_LOGRADOURO_EST\"',
    '\"NOME_BAIRRO_EST\"',
    '\"NUMR_CEP_EST\"',
    '\"CODG_UF_EST\"',
    '\"CODG_MUNICIPIO_EST\"',
    '\"NUMR_DDD_FONE_1_EST\"',
    '\"NUMR_TELEFONE_1_EST\"',
    '\"NUMR_DDD_FONE_2_EST\"',
    '\"NUMR_TELEFONE_2_EST\"',
    '\"NUMR_DDD_FAX_EST\"',
    '\"NUMR_FAX_EST\"',
    '\"DESC_EMAIL_EST\"',
    '\"DESC_SITUACAO_ESPECIAL_EST\"',
    '\"DATA_SITUACAO_ESPECIAL_EST\"',
    '\"CODG_HASH_EST\"',
    '\"NUMR_ATUALIZACAO_EST\"'
]

# Foreign key column names
foreign_key_columns = [
    '\"PES_ESTABELECIMENTO_ID_EMPRESA_fkey\"', '\"PES_ESTABELECIMENTO_ID_SIT_CADASTRAL_fkey\"',
    '\"PES_ESTABELECIMENTO_ID_MOT_SIT_fkey\"', '\"PES_ESTABELECIMENTO_ID_PAIS_fkey\"',
    '\"PES_ESTABELECIMENTO_ID_MUNIC_fkey\"'
]

# Função para inserir o log no arquivo de texto
def inserir_log(carga, numero_atualizacao, arquivo, data_atualizacao, dados_originais, dados_atualizados, operacao):
    try:
        with open('/Users/rvazsilva/Temporario/Receita Federal/LogPessoa.txt', 'a') as arquivo_log:
            if arquivo_log.tell() == 0:
                # Cabeçalho do arquivo de log
                arquivo_log.write('carga;numr_atualizacao;arquivo;data_atualizacao;dados_originais;dados_atualizados;oper\n')

            linha = f'{carga};{numero_atualizacao};{arquivo};{data_atualizacao};{dados_originais};{dados_atualizados};{operacao}\n'
            arquivo_log.write(linha)
        return
    
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("Erro: ", e, "Tipo: ", exc_type, "Nome: ",fname, "Linha: ", exc_tb.tb_lineno)

# Function to insert or update data in the database
def insert_or_update_data(query, values):
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()
    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    conn.close()

def obter_id_empresa (conexao, numr_cnpj_basico, hash_linha, numero_atualizacao):
# Buscar o ID da tabela PES_EMPRESA
    cursor = conexao.cursor()
    query = """
        SELECT \"PES_ID_EMPRESA\"
        FROM basespessoas.\"PES_EMPRESA\"
        WHERE \"NUMR_CNPJ_BASICO_EMP\" = %s
    """
    cursor.execute(query, (numr_cnpj_basico,))
    resultado = cursor.fetchone()

    if resultado:
        id_empresa = resultado[0]
    else:
        # Inserir na tabela PES_EMPRESA
        query = """
            INSERT INTO basespessoas.\"PES_EMPRESA\" (\"NUMR_CNPJ_BASICO_EMP\", \"NOME_RAZAO_SOCIAL_EMP\", \"CODG_HASH_EMP\", \"NUMR_ATUALIZACAO_EMP\")
            VALUES (%s, 'Empresa não encontrada', %s, %s)
            RETURNING \"PES_ID_EMPRESA\"
        """
        cursor.execute(query, (numr_cnpj_basico, hash_linha, numero_atualizacao,))
        id_empresa = cursor.fetchone()[0]
    return id_empresa

def obter_id_sit_cadastr (conexao, codg_sit_cadastr):
# Buscar o ID da tabela PES_SIT_CADASTRAL para ESTABELECIMENTO
    cursor = conexao.cursor()
    query = """
        SELECT \"PES_ID_SIT_CADASTRAL\"
        FROM basespessoas.\"PES_SIT_CADASTRAL\"
        WHERE \"CODG_SIT_CADASTRAL_SCD\" = %s
    """
    cursor.execute(query, (codg_sit_cadastr,))
    resultado = cursor.fetchone()

    if resultado:
        id_sit_cadastr = resultado[0]
    else:
        # Inserir na tabela PES_SIT_CADASTRAL para ESTABELECIMENTO
        query = """
            INSERT INTO basespessoas.\"PES_SIT_CADASTRAL\" (
            \"CODG_SIT_CADASTRAL_SCD\", 
            \"DESC_SIT_CADASTRAL_SCD\")
            VALUES (%s, 'Não encontrado')
            RETURNING \"PES_ID_SIT_CADASTRAL\"
        """
        cursor.execute(query, (codg_sit_cadastr,))
        id_sit_cadastr = cursor.fetchone()[0]

    return (id_sit_cadastr)

def obter_id_mot_sit (conexao, codg_mot_sit):
# Buscar o ID da tabela PES_SIT_CADASTRAL para ESTABELECIMENTO
    cursor = conexao.cursor()
    query = "SELECT \"PES_ID_MOT_SIT\" FROM basespessoas.\"PES_MOT_SIT\" WHERE \"CODG_MOT_SIT_MST\" = %s"
    cursor.execute(query, (codg_mot_sit,))
    resultado = cursor.fetchone()

    if resultado:
        id_mot_sit = resultado[0]
    else:
        # Inserir na tabela PES_MOT_SIT para ESTABELECIMENNTO
        query = """
            INSERT INTO basespessoas.\"PES_MOT_SIT\" (
            \"CODG_MOT_SIT_MST\", 
            \"DESC_MOT_SIT_MST\")
            VALUES (%s, 'Motivo da situação cadastral não encontrado')
            RETURNING \"PES_ID_MOT_SIT\"
        """
        cursor.execute(query, (codg_mot_sit,))
        id_mot_sit = cursor.fetchone()[0]

    return (id_mot_sit)

# Função para obter o ID do país
def obter_id_pais(conexao, codg_pais):
    cursor = conexao.cursor()
    query = "SELECT \"PES_ID_PAIS\" FROM basespessoas.\"PES_PAIS\" WHERE \"CODG_PAIS_PAI\" = %s"
    cursor.execute(query, (codg_pais,))
    resultado = cursor.fetchone()

    if resultado:
        id_pais = resultado[0]
    else:
        # País não cadastrado, inserir na tabela
        query = "INSERT INTO basespessoas.\"PES_PAIS\" (\"CODG_PAIS_PAI\", \"NOME_PAIS_PAI\") VALUES (%s, %s) RETURNING \"PES_ID_PAIS\""
        cursor.execute(query, (codg_pais, 'Codigo pais não cadastrado'))
        id_pais = cursor.fetchone()[0]
        conexao.commit()

    cursor.close()
    return id_pais

# Função para obter o ID do Município
def obter_id_municipio(conexao, codg_municipio):
    cursor = conexao.cursor()
    query = "SELECT \"PES_ID_MUNIC\" FROM basespessoas.\"PES_MUNIC\" WHERE \"CODG_MUNIC_MUN\" = %s"
    cursor.execute(query, (codg_municipio,))
    resultado = cursor.fetchone()

    if resultado:
        id_municipio = resultado[0]
    else:
        # País não cadastrado, inserir na tabela
        query = "INSERT INTO basespessoas.\"PES_MUNIC\" (\"CODG_MUNIC_MUN\", \"NOME_MUNIC_MUN\") VALUES (%s, %s)"
        id_municipio = cursor.fetchone()[0]
        conexao.commit()

    cursor.close()
    return id_municipio

# Função para obter o ID do Município
def obter_id_estabelecimento(conexao, numr_cnpj_completo, linha):
    cursor = conexao.cursor()

    # Check if the record already exists in the table
    query = f"SELECT \"PES_ID_ESTABELECIMENTO\" FROM {table_name} WHERE \"NUMR_CNPJ_COMPLETO_EST\" = %s"
    cursor.execute(query, (linha[0] + linha[1] + linha[2],))
    resultado = cursor.fetchone()

    if resultado:
        id_estabelecimento == resultado[0]
    else:
        # Buscar os IDs das tabelas relacionadas
        numr_cnpj_basico = linha[0]
        codg_sit_cadastro = linha[5]
        codg_mot_sit = linha[7]
        if linha[9] == "" or linha[9] == None:
            codg_pais = "999"
        else:
            codg_pais = linha[9]
        codg_municipio = linha[20]

        id_empresa = obter_id_empresa(conexao, numr_cnpj_basico, hash_linha, numero_atualizacao)
        id_sit_cadastr = obter_id_sit_cadastr(conexao, codg_sit_cadastro)
        id_mot_sit = obter_id_mot_sit(conexao, codg_mot_sit)
        id_pais = obter_id_pais(conexao, codg_pais)
        id_minicipio = obter_id_municipio(conexao, codg_municipio)

        # Record does not exist, insert a new record
        colunas_tabela = ", ".join(f'{column_names[i]}' for i in range(len(column_names)))
        colunas_tabela += ", " + ", ".join(f'{foreign_key_columns[i]}' for i in range(len(foreign_key_columns)))
        query = f'INSERT INTO {table_name} ({colunas_tabela}) VALUES ({", ".join(["%s"] * len(colunas_tabela.split(", ")))}) RETURNING \"PES_ID_ESTABELECIMENTO\"'
        linha.insert(3, linha[0] + linha[1] + linha[2])
        dados_atualizados = ", ".join(f'{linha[i]}' for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30])
        dados = (((dados_atualizados + ", " + f'{hash_linha}').split(", ")), numero_atualizacao, id_empresa, id_sit_cadastr, id_mot_sit, id_pais, id_minicipio,)

        cursor.execute(query, (dados,))

        # Registrar inclusão no arquivo de log
        inserir_log ("ESTABE", numero_atualizacao, arquivo, data_e_hora_atuais, "", dados_atualizados, "E")

    cursor.close()
    return id_estabelecimento

# Read and process the CSV file
with open(caminho, "r", encoding=encoding) as file:
    csv_reader = csv.reader(file, delimiter=delimiter, quotechar=quotechar)
    next(csv_reader)  # Skip header row
    counter = 0
    for row in csv_reader:
        counter += 1
        cnpj = row[0] + row[1] + row[2]
        cnae = row[10]
        cnae_list = row[11].split(",")
        
        # Check if PES_ESTABELECIMENTO record exists
        query = f"SELECT PES_ID_ESTABELECIMENNTO FROM basespessoas.\"PES_ESTABELECIMENTO\" WHERE NUMR_CNPJ_COMPLETO_EST = '{cnpj}'"
        insert_or_update_data(query, ())
        
        # Check if PES_CNAE record exists
        query = f"SELECT PES_ID_CNAE FROM basespessoas.\"PES_CNAE\" WHERE CODG_CNAE_CNE = '{cnae}'"
        insert_or_update_data(query, ())
        
        # Check if PES_CNAE_ESTABEL record exists
        query = f"SELECT * FROM basespessoas.\"PES_CNAE_ESTABEL\" WHERE PES_CNAE_ESTABEL_ID_CNAE_fkey = id_cnae AND PES_CNAE_ESTABEL_ID_ESTABEL_fkey = id_estabelecimento"
        insert_or_update_data(query, ())
        
        # Insert or update PES_CNAE_ESTABEL record
        query = "INSERT INTO basespessoas.\"PES_CNAE_ESTABEL\" (INDR_CNAE_ESTABEL_CES, PES_CNAE_ESTABEL_ID_CNAE_fkey, PES_CNAE_ESTABEL_ID_ESTABEL_fkey, INDR_ATIVO_CES, NUMR_ATUALIZACAO_CES) VALUES (%s, %s, %s, %s, %s)"
        values = ("0", id_cnae, id_estabelecimento, True, numero_atualizacao)
        insert_or_update_data(query, values)
        
        # Process CNAE codes
        for cnae_code in cnae_list:
            # Check if PES_CNAE record exists
            query = f"SELECT PES_ID_CNAE FROM basespessoas.\"PES_CNAE\" WHERE CODG_CNAE_CNE = '{cnae_code}'"
            insert_or_update_data(query, ())
            
            # Check if PES_CNAE_ESTABEL record exists
            query = f"SELECT * FROM basespessoas.\"PES_CNAE_ESTABEL\" WHERE PES_CNAE_ESTABEL_ID_CNAE_fkey = id_cnae AND PES_CNAE_ESTABEL_ID_ESTABEL_fkey = id_estabelecimento"
            insert_or_update_data(query, ())
            
            # Insert or update PES_CNAE_ESTABEL record
            query = "INSERT INTO basespessoas.\"PES_CNAE_ESTABEL\" (INDR_CNAE_ESTABEL_CES, PES_CNAE_ESTABEL_ID_CNAE_fkey, PES_CNAE_ESTABEL_ID_ESTABEL_fkey, INDR_ATIVO_CES, NUMR_ATUALIZACAO_CES) VALUES (%s, %s, %s, %s, %s)"
            values = ("0", id_cnae, id_estabelecimento, True, numero_atualizacao)
            insert_or_update_data(query, values)
        
        # Write log entry for insertion
        inserir_log("I", ",".join(row[:11]))
        
        # Commit every 1000 insertions
        if counter % 1000 == 0:
            conn.commit()
            print(f"{counter} records processed and committed.")
    
    # Commit remaining insertions
    conn.commit()
    print(f"All {counter} records processed and committed.")