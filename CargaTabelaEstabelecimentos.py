import csv
import hashlib
import psycopg2
from datetime import datetime
import sys, os

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

def obter_fk_id (conexao, hash_linha, numero_atualizacao, numr_cnpj_basico,codg_sit_cadastro, codg_mot_sit, codg_pais, codg_municipio):
    id_empresa = obter_id_empresa(conexao, numr_cnpj_basico, hash_linha, numero_atualizacao)
    id_sit_cadastr = obter_id_sit_cadastr(conexao, codg_sit_cadastro)
    id_mot_sit = obter_id_mot_sit(conexao, codg_mot_sit)
    id_pais = obter_id_pais(conexao, codg_pais)
    id_municipio = obter_id_municipio(conexao, codg_municipio)

    return (id_empresa, id_sit_cadastr, id_mot_sit, id_pais, id_municipio)

def inserir_atualizar_dados(caminho, arquivo, numero_atualizacao):
    caminho_completo = f"{caminho}/{arquivo}"
    try:
        # Database connection details
        database = "BasesPessoas"
        user = "postgres"
        password = "postgres"
        host = "localhost"
        port = "5432"

        # CSV file settings
        delimiter = ";"
        quotechar = '"'
        encoding = "ISO 8859-1"

        # Table and column names
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

        # Create a connection to the PostgreSQL database
        conexao = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        cursor = conexao.cursor()

        # Open the CSV file and read its contents
        with open(caminho_completo, "r", encoding=encoding) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=delimiter, quotechar=quotechar)

            # Initialize counters
            contador_lidos = 0
            contador_gravados = 0
            data_e_hora_object = datetime.now()           
            data_e_hora_atuais = data_e_hora_object.strftime ('%Y-%m-%d %H:%M:%S')

            try:
                # Iterate over each linha in the CSV file
                for linha in csv_reader:
                    contador_lidos += 1

                    # Fazer commit a cada 1000 inserções

                    if contador_gravados % 1000 == 0:
                        conexao.commit()

                    if contador_lidos % 1000 == 0:
                        data_e_hora_object = datetime.now()           
                        data_e_hora_atuais = data_e_hora_object.strftime ('%Y-%m-%d %H:%M:%S')
                        print(f'Registros lidos: {contador_lidos}')
                        print(f'Registros gravados: {contador_gravados}')

                    # Calculate the hash for the linha
                    hash_string = "".join(linha[i] for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29])
                    hash_linha = hashlib.md5(hash_string.encode()).hexdigest()

                    # Buscar os IDs das tabelas relacionadas
                    numr_cnpj_basico = linha[0]
                    codg_sit_cadastro = linha[5]
                    codg_mot_sit = linha[7]
                    if linha[9] == "" or linha[9] == None:
                        codg_pais = "999"
                    else:
                        codg_pais = linha[9]
                    codg_municipio = linha[20]

                    # Check if the record already exists in the table
                    query = f"SELECT * FROM {table_name} WHERE \"NUMR_CNPJ_COMPLETO_EST\" = %s"
                    cursor.execute(query, (linha[0] + linha[1] + linha[2],))
                    resultado = cursor.fetchone()

                    #print("Resultado: ", resultado)
                    #print("Hash: ", hash_linha)
                    #print("Contador: ",contador_lidos, " - ", contador_gravados)
                    #wait = input("Pressione Enter para continuar...")

                    if resultado:
                        # Compare the hashes
                        if resultado[35] == hash_linha:
                            # Hashes are equal, skip to the next linha
                            continue
                        else:
                            (id_empresa, id_sit_cadastr, id_mot_sit, id_pais, id_municipio) = obter_fk_id (conexao, hash_linha, numero_atualizacao, numr_cnpj_basico,codg_sit_cadastro, codg_mot_sit, codg_pais, codg_municipio)
                            # Hashes are different, update the record
                            colunas_tabela = ", ".join(f'{column_names[i]} = %s' for i in range(len(column_names)))
                            colunas_tabela += ", " + ", ".join(f'{foreign_key_columns[i]} = %s' for i in range(len(foreign_key_columns)))
                            query = f'UPDATE {table_name} SET {colunas_tabela} WHERE \"PES_ID_ESTABELECIMENTO\" = {"%s"}'
                            #colunas_tabela = ", ".join(f'{colunas_tabela[i]}' + " = " + "%s" for i in range(len(colunas_tabela)))
                            #query = f'UPDATE {table_name} SET {", ".join([{colunas_tabela} " = " "%s"] * len(colunas_tabela.split(", ")))} WHERE \"PES_ID_ESTABELECIMENTO\" = {"%s"}'
                            linha.insert(3, (linha[0] + linha[1] + linha[2]))
                            #dados = (((dados_atualizados + ", " + f'{hash_linha}').split(", ")), numero_atualizacao, id_empresa, id_sit_cadastr, id_mot_sit, id_pais, id_municipio,)

                            #print ("Linha: ", (linha[0], linha[1], linha[2], linha[3], linha[4], linha[5], linha[6],
                            #                linha[7], linha[8], linha[9], linha[10], linha[11], linha[14], linha[15], linha[16],
                            #                linha[17], linha[18], linha[19], linha[20], linha[21], linha[22], linha[23], linha[24],
                            #                linha[25], linha[26], linha[27], linha[28], linha[29], linha[30],
                            #                hash_linha, numero_atualizacao, id_empresa, id_sit_cadastr, id_mot_sit, id_pais, id_municipio, resultado[0],))
                            #print ("Query: ", query)
                            #wait = input("Pressione Enter para continuar...")
            
                            dados_atualizados = ", ".join(f'{linha[i]}' for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30])
                            cursor.execute(query, (linha[0], linha[1], linha[2], linha[3], linha[4], linha[5], linha[6], linha[7], linha[8], linha[9], linha[10], 
                                               linha[11], linha[14], linha[15], linha[16], linha[17], linha[18], linha[19], linha[20], 
                                               linha[21], linha[22], linha[23], linha[24], linha[25], linha[26], linha[27], linha[28], linha[29], linha[30],
                                               hash_linha, numero_atualizacao, id_empresa, id_sit_cadastr, id_mot_sit, id_pais, id_municipio, resultado[0]))
                            
                            # Registrar inclusão no arquivo de log
                            dados_originais = ", ".join(f'{resultado[i]}' for i in [2, 3, 4, 5, 6, 7, 8, 10, 11, 13, 14, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 27, 28, 29, 30, 31, 32, 33, 34])
                            inserir_log ("ESTABE", numero_atualizacao, arquivo, data_e_hora_atuais, dados_originais, dados_atualizados, "U")

                            contador_gravados += 1
                    else:
                        # Record does not exist, insert a new record
                        (id_empresa, id_sit_cadastr, id_mot_sit, id_pais, id_municipio) = obter_fk_id (conexao, hash_linha, numero_atualizacao, numr_cnpj_basico,codg_sit_cadastro, codg_mot_sit, codg_pais, codg_municipio)
                        colunas_tabela = ", ".join(f'{column_names[i]}' for i in range(len(column_names)))
                        colunas_tabela += ", " + ", ".join(f'{foreign_key_columns[i]}' for i in range(len(foreign_key_columns)))
                        query = f'INSERT INTO {table_name} ({colunas_tabela}) VALUES ({", ".join(["%s"] * len(colunas_tabela.split(", ")))}) RETURNING \"PES_ID_ESTABELECIMENTO\"'
                        linha.insert(3, (linha[0] + linha[1] + linha[2]))

                        dados_atualizados = (", ".join(f'{linha[i]}' for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]))
                        cursor.execute(query, (linha[0], linha[1], linha[2], linha[3], linha[4], linha[5], linha[6], linha[7], linha[8], linha[9], linha[10], 
                                               linha[11], linha[14], linha[15], linha[16], linha[17], linha[18], linha[19], linha[20], 
                                               linha[21], linha[22], linha[23], linha[24], linha[25], linha[26], linha[27], linha[28], linha[29], linha[30],
                                               hash_linha, numero_atualizacao, id_empresa, id_sit_cadastr, id_mot_sit, id_pais, id_municipio,))

                        # Registrar inclusão no arquivo de log
                        inserir_log ("ESTABE", numero_atualizacao, arquivo, data_e_hora_atuais, "", dados_atualizados, "I")

                        contador_gravados += 1

                # Commit any remaining changes
                conexao.commit()

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print("Erro: ", e, "Nome: ",fname, "Linha: ", exc_tb.tb_lineno)
                print("Dados: ", (linha[0], linha[1], linha[2], linha[3], linha[4], linha[5], linha[6], linha[7], linha[8], linha[9], linha[10], 
                                               linha[11], linha[14], linha[15], linha[16], linha[17], linha[18], linha[19], linha[20], 
                                               linha[21], linha[22], linha[23], linha[24], linha[25], linha[26], linha[27], linha[28], linha[29], linha[30],
                                               hash_linha, numero_atualizacao, id_empresa, id_sit_cadastr, id_mot_sit, id_pais, id_municipio,))
                print ("Registro: ", resultado)
                pass

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("Erro: ", e, "Tipo: ", exc_type, "Nome: ",fname, "Linha: ", exc_tb.tb_lineno)
        pass

    finally:
        cursor.close()
        conexao.close()

def main():
    # Pedir o caminho do arquivo CSV e o número da atualização
    caminho = input("Digite o caminho do arquivo CSV: ")
    numero_atualizacao = input("Digite o número da atualização: ")

    # Chamar a função para inserir/atualizar os dados
    arquivo = "K3241.K03200Y1.D40113.ESTABELE"
    print("------------------ K3241.K03200Y1.D40113.ESTABELE ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y2.D40113.ESTABELE"
    print("------------------ K3241.K03200Y2.D40113.ESTABELE ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y3.D40113.ESTABELE"
    print("------------------ K3241.K03200Y3.D40113.ESTABELE ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y4.D40113.ESTABELE"
    print("------------------ K3241.K03200Y4.D40113.ESTABELE ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y5.D40113.ESTABELE"
    print("------------------ K3241.K03200Y5.D40113.ESTABELE ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y6.D40113.ESTABELE"
    print("------------------ K3241.K03200Y6.D40113.ESTABELE ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y7.D40113.ESTABELE"
    print("------------------ K3241.K03200Y7.D40113.ESTABELE ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y8.D40113.ESTABELE"
    print("------------------ K3241.K03200Y8.D40113.ESTABELE ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    #arquivo = "K3241.K03200Y9.D40113.ESTABELE"
    #print("------------------ K3241.K03200Y9.D40113.ESTABELE ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y0.D40113.ESTABELE"
    print("------------------ K3241.K03200Y0.D40113.ESTABELE ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

if __name__ == "__main__":
    main()