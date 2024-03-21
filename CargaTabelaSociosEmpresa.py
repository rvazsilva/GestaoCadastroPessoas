import csv
import hashlib
import psycopg2
from datetime import datetime
import sys, os

# Função para calcular o hash MD5
def calcular_hash(dados):
    md5 = hashlib.md5()
    md5.update(dados.encode('utf-8'))
    return md5.hexdigest()

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

def obter_id_socio(conexao, linha, hash_linha, numero_atualizacao):
# Buscar o ID da tabela PES_SOCIOS
    cursor = conexao.cursor()
    query = """
        SELECT \"PES_ID_SOCIOS\"
        FROM basespessoas.\"PES_SOCIOS\"
        WHERE \"NOME_SOCIO_SOC\" = %s
        AND \"NUMR_CNPJ_CPF_SOC\" = %s
    """
    cursor.execute(query, (linha[2], linha[3],))
    resultado = cursor.fetchone()

    if resultado:
        id_socios = resultado[0]
    else:
        # Inserir na tabela PES_SOCIOS
        id_pais = obter_id_pais (conexao, linha[6])
        id_faixa_etaria = obter_id_faixa_etaria(conexao, linha[10])
        query = """
            INSERT INTO basespessoas.\"PES_SOCIOS\" (\"INDR_TIPO_PESSOA_SOC\", 
            \"NOME_SOCIO_SOC\", 
            \"NUMR_CNPJ_CPF_SOC\", 
            \"CODG_PAIS_SOC\",
            \"PES_SOCIOS_ID_PAIS_fkey\", 
            \"INDR_FAIXA_ETARIA_SOC\", 
            \"PES_SOCIOS_ID_FAIXA_ETARIA_fkey\", 
            \"CODG_HASH_SOC\", 
            \"NUMR_ATUALIZACAO_SOC\")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING \"PES_ID_SOCIOS\"
        """
        cursor.execute(query, (linha[1], linha[2], linha[3], linha[6], id_pais, linha[10], id_faixa_etaria, hash_linha, numero_atualizacao,))
        id_socios = cursor.fetchone()[0]
    
    return id_socios

def obter_id_qualif_socio(conexao, codg_qualif_socio):
    # Buscar o ID da tabela PES_QUALIF_PESSOA para QUALIF_SOCIO
    cursor = conexao.cursor()
    query = """
        SELECT \"PES_ID_QUALIF_PESSOA\"
        FROM basespessoas.\"PES_QUALIF_PESSOA\"
        WHERE \"CODG_QUALIF_PESSOA_QLF\" = %s
    """
    cursor.execute(query, (codg_qualif_socio,))
    resultado = cursor.fetchone()

    if resultado:
        id_qualif_socio = resultado[0]
    else:
        # Inserir na tabela PES_QUALIF_PESSOA para QUALIF_SOCIO
        query = """
            INSERT INTO basespessoas.\"PES_QUALIF_PESSOA\" (\"CODG_QUALIF_PESSOA_QLF\", \"DESC_QUALIF_PESSOA_QLF\")
            VALUES (%s, 'Qualificação sócio não encontrada')
            RETURNING \"PES_ID_QUALIF_PESSOA\"
        """
        cursor.execute(query, (codg_qualif_socio,))
        id_qualif_socio = cursor.fetchone()[0]
    return id_qualif_socio

def obter_id_qualif_repres(conexao, codg_qualif_repres):
# Buscar o ID da tabela PES_QUALIF_PESSOA para QUALIF_REPRES
    cursor = conexao.cursor()
    query = """
        SELECT \"PES_ID_QUALIF_PESSOA\"
        FROM basespessoas.\"PES_QUALIF_PESSOA\"
        WHERE \"CODG_QUALIF_PESSOA_QLF\" = %s
    """
    cursor.execute(query, (codg_qualif_repres,))
    resultado = cursor.fetchone()

    if resultado:
        id_qualif_repres = resultado[0]
    else:
        # Inserir na tabela PES_QUALIF_PESSOA para QUALIF_REPRES
        query = """
            INSERT INTO basespessoas.\"PES_QUALIF_PESSOA\" (
            \"CODG_QUALIF_PESSOA_QLF\", 
            \"DESC_QUALIF_PESSOA_QLF\")
            VALUES (%s, 'Qualificação representante não encontrada')
            RETURNING \"PES_ID_QUALIF_PESSOA\"
        """
        cursor.execute(query, (codg_qualif_repres,))
        id_qualif_repres = cursor.fetchone()[0]

    return (id_qualif_repres)

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
        query = """INSERT INTO basespessoas.\"PES_PAIS\" (\"CODG_PAIS_PAI\", \"NOME_PAIS_PAI\") VALUES (%s, %s)"#, 
            (codg_pais, 'Codigo pais não cadastrado')# RETURNING \"PES_ID_PAIS\"
            """
        cursor.execute(query, (codg_pais, 'Codigo pais não cadastrado',))
        id_pais = cursor.fetchone()[0]

    return id_pais

# Função para obter o ID da faixa etária
def obter_id_faixa_etaria(conexao, indr_faixa_etaria):
    cursor = conexao.cursor()
    query = "SELECT \"PES_ID_FAIXA_ETARIA\" FROM basespessoas.\"PES_FAIXA_ETARIA\" WHERE \"CODG_FAIXA_ETARIA_FET\" = %s"
    cursor.execute(query, (indr_faixa_etaria,))
    resultado = cursor.fetchone()

    if resultado:
        id_faixa_etaria = resultado[0]
    else:
        # Faixa etária não cadastrada, inserir na tabela
        query = "INSERT INTO basespessoas.\"PES_FAIXA_ETARIA\" (\"CODG_FAIXA_ETARIA_FET\", \"DESC_FAIXA_ETARIA_FET\") VALUES (%s, %s)"# RETURNING \"PES_ID_FAIXA_ETARIA\""
        cursor.execute(query, (indr_faixa_etaria, 'Codigo faixa etária não cadastrado',))
        id_faixa_etaria = cursor.fetchone()[0]

    return id_faixa_etaria

# Função principal
def inserir_atualizar_dados(caminho, arquivo, numero_atualizacao):
    caminho_completo = f"{caminho}/{arquivo}"

    # Conectar ao banco de dados PostgreSQL
    conexao = psycopg2.connect(host='localhost', port='5432', database='BasesPessoas', user='postgres', password='postagres')
    cursor = conexao.cursor()

    try:
        with open(caminho_completo, 'r', encoding='iso-8859-1') as arquivo_csv:
            leitor_csv = csv.reader(arquivo_csv, delimiter=';', quotechar='"')

            contador_lidos = 0
            contador_gravados = 0
            data_e_hora_object = datetime.now()           
            data_e_hora_atuais = data_e_hora_object.strftime ('%Y-%m-%d %H:%M:%S')

            try:

                for linha in leitor_csv:
                    contador_lidos += 1

                    # Fazer commit a cada 1000 inserções
                    if contador_gravados != 0:
                        if contador_gravados % 1000 == 0:
                            data_e_hora_object = datetime.now()           
                            data_e_hora_atuais = data_e_hora_object.strftime ('%Y-%m-%d %H:%M:%S')
                            print(f'Registros lidos: {contador_lidos}')
                            print(f'Registros gravados: {contador_gravados}')
                            conexao.commit()
                    else:
                        if contador_lidos % 1000 == 0:
                            data_e_hora_object = datetime.now()           
                            data_e_hora_atuais = data_e_hora_object.strftime ('%Y-%m-%d %H:%M:%S')
                            print(f'Registros lidos: {contador_lidos}')
                            print(f'Registros gravados: {contador_gravados}')
                            #conexao.commit()

                    # Calcular o hash da linha
                    hash_linha = calcular_hash(linha [4] + linha[5] + linha [7] + linha [8] + linha [9])

                    # Buscar os IDs das tabelas relacionadas
                    codg_qualif_socio = linha[4]
                    codg_qualif_repres = linha[9]
                    numr_cnpj_basico = linha[0]

                    id_empresa = obter_id_empresa(conexao, numr_cnpj_basico, hash_linha, numero_atualizacao)
                    id_socios = obter_id_socio(conexao, linha, hash_linha, numero_atualizacao)
                    id_qualif_socio = obter_id_qualif_socio(conexao, codg_qualif_socio)
                    id_qualif_repres = obter_id_qualif_repres(conexao, codg_qualif_repres)

                    # Inserir ou atualizar os dados na tabela PES_SOCIOS_EMPRESA
                    # Verificar se o registro já existe na tabela
                    query = """
                        SELECT \"CODG_QUALIFIC_SOCIO_SEM\",
                        \"DATA_ENTRADA_SOCIEDADE_SEM\",
                        \"NUMR_CNPJ_CPF_REPRES_SEM\", 
                        \"NOME_REPRES_SEM\",
                        \"CODG_QUALIFIC_REPRES_SEM\",
                        \"CODG_HASH_SEM\"
                    FROM basespessoas.\"PES_SOCIOS_EMPRESA\"
                    WHERE \"PES_SOCIOS_EMPRESA_ID_EMPRESA_fkey\" = %s
                    AND \"PES_SOCIOS_EMPRESA_ID_SOCIOS_fkey\" = %s
                    """
                    cursor.execute(query, (id_empresa, id_socios,))
                    resultado = cursor.fetchone()

                    dados_atualizados = (linha [4], linha[5], linha [7], linha [8], linha [9])

                    if resultado:
                        dados_originais = (resultado[0], resultado[1], resultado[2], resultado[3], resultado[4], resultado[5])

                        # Registro já existe, comparar os hashes
                        if resultado[5] == hash_linha:
                            # Hashes iguais, não atualiza dados
                            continue
                        else:
                            # Hashes diferentes, atualizar os dados
                            query = """
                            UPDATE basespessoas.\"PES_SOCIOS_EMPRESA\"
                                SET \"CODG_QUALIFIC_SOCIO_SEM\" = %s,
                                    \"PES_SOCIOS_EMPRESA_ID_QUALIF_SOCIO_fkey\" = %s,
                                    \"DATA_ENTRADA_SOCIEDADE_SEM\" = %s,
                                    \"NUMR_CNPJ_CPF_REPRES_SEM\" = %s,
                                    \"NOME_REPRES_SEM\" = %s,
                                    \"CODG_QUALIFIC_REPRES_SEM\" = %s,
                                    \"PES_SOCIOS_EMPRESA_ID_QUALIF_REPRES_fkey\" = %s,
                                    \"CODG_HASH_SEM\" = %s,
                                    \"NUMR_ATUALIZACAO_SEM\" = %s
                                WHERE \"PES_SOCIOS_EMPRESA_ID_EMPRESA_fkey\" = %s
                                AND \"PES_SOCIOS_EMPRESA_ID_SOCIOS_fkey\" = %s
                            """
                            
                            cursor.execute (query, (linha [4], id_qualif_socio, linha[5], linha [7], linha [8], linha [9], id_qualif_repres, hash_linha, numero_atualizacao, id_empresa, id_socios,))
                            contador_gravados += 1

                            # Registrar inclusão no arquivo de log
                            inserir_log ("SOCEMP", numero_atualizacao, arquivo, data_e_hora_atuais, dados_originais, dados_atualizados, "U")
                    else:
                        # Registro não existe, inserir os dados                    
                        dados_originais = ""
                        query = """
                            INSERT INTO basespessoas.\"PES_SOCIOS_EMPRESA\" 
                                (\"PES_SOCIOS_EMPRESA_ID_EMPRESA_fkey\",
                                \"PES_SOCIOS_EMPRESA_ID_SOCIOS_fkey\",
                                \"CODG_QUALIFIC_SOCIO_SEM\",
                                \"PES_SOCIOS_EMPRESA_ID_QUALIF_SOCIO_fkey\", 
                                \"DATA_ENTRADA_SOCIEDADE_SEM\", 
                                \"NUMR_CNPJ_CPF_REPRES_SEM\", 
                                \"NOME_REPRES_SEM\", 
                                \"CODG_QUALIFIC_REPRES_SEM\",
                                \"PES_SOCIOS_EMPRESA_ID_QUALIF_REPRES_fkey\", 
                                \"CODG_HASH_SEM\", 
                                \"NUMR_ATUALIZACAO_SEM\")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        cursor.execute(query, (id_empresa, id_socios, linha [4], id_qualif_socio, linha[5], linha [7], linha [8], linha [9], id_qualif_repres, hash_linha, numero_atualizacao,))
                        contador_gravados += 1

                        # Registrar inclusão no arquivo de log
                        inserir_log ("SOCEMP", numero_atualizacao, arquivo, data_e_hora_atuais, dados_originais, dados_atualizados, "I")
                        
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print("Erro: ", e, "Tipo: ", exc_type, "Nome: ",fname, "Linha: ", exc_tb.tb_lineno)
                print("Registro: ", linha)
                pass
        
            # Fazer commit das inserções restantes
            conexao.commit()

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
    arquivo = "K3241.K03200Y1.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y1.D40113.SOCIOCSV ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y2.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y2.D40113.SOCIOCSV ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y3.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y3.D40113.SOCIOCSV ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y4.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y4.D40113.SOCIOCSV ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y5.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y5.D40113.SOCIOCSV ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y6.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y6.D40113.SOCIOCSV ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y7.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y7.D40113.SOCIOCSV ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y8.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y8.D40113.SOCIOCSV ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y9.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y9.D40113.SOCIOCSV ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y0.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y0.D40113.SOCIOCSV ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

if __name__ == "__main__":
    main()