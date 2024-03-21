import csv
import hashlib
import psycopg2
from datetime import datetime

# Função para calcular o hash MD5
def calcular_hash(dados):
    md5 = hashlib.md5()
    md5.update(dados.encode('utf-8'))
    return md5.hexdigest()

# Função para inserir o log no arquivo de texto
def inserir_log(carga, numero_atualizacao, arquivo, data_atualizacao, dados_originais, dados_atualizados, operacao):
    with open('/Users/rvazsilva/Temporario/Receita Federal/LogPessoa.txt', 'a') as arquivo_log:
        if arquivo_log.tell() == 0:
            # Cabeçalho do arquivo de log
            arquivo_log.write('carga;numr_atualizacao;arquivo;data_atualizacao;dados_originais;dados_atualizados;oper\n')

        linha = f'{carga};{numero_atualizacao};{arquivo};{data_atualizacao};{dados_originais};{dados_atualizados};{operacao}\n'
        arquivo_log.write(linha)

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
        query = "INSERT INTO basespessoas.\"PES_PAIS\" (\"CODG_PAIS_PAI\", \"NOME_PAIS_PAI\") VALUES (%s, %s)"#, (codg_pais, 'Codigo pais não cadastrado')# RETURNING \"PES_ID_PAIS\""
        cursor.execute(query, (codg_pais, 'Codigo pais não cadastrado'))
        id_pais = cursor.fetchone()[0]
        conexao.commit()

    cursor.close()
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
        cursor.execute(query, indr_faixa_etaria, 'Codigo faixa etária não cadastrado')
        id_faixa_etaria = cursor.fetchone()[0]
        conexao.commit()

    cursor.close()
    return id_faixa_etaria

# Função principal
def inserir_atualizar_dados(caminho, arquivo, numero_atualizacao):
    # Montar o caminho completo do arquivo CSV
    caminho_completo = f"{caminho}/{arquivo}"

    # Conectar ao banco de dados PostgreSQL
    conexao = psycopg2.connect(database="BasesPessoas", user="postgres", password="postgres", host="localhost", port="5432")
    cursor = conexao.cursor()

    # Abrir o arquivo CSV
    try:
        with open(caminho_completo, 'r', encoding='ISO 8859-1') as arquivo_csv:
            leitor_csv = csv.reader(arquivo_csv, delimiter=';', quotechar='"')

            # Inicializar as contadores
            contador_lidos = 0
            contador_gravados = 0
            total_registros = 0
            data_e_hora_object = datetime.now()           
            data_e_hora_atuais = data_e_hora_object.strftime ('%Y-%m-%d %H:%M:%S')

            # Pular o cabeçalho do arquivo CSV
            #next(leitor_csv)

            #Obtem data e hora atuais
            data_e_hora_atuais = datetime.now()

            try:

                # Percorrer as linhas do arquivo CSV
                for linha in leitor_csv:
                    registros_lidos += 1

                    # Calcular o hash MD5
                    dados = ';'.join(linha)
                    hash_md5 = calcular_hash(dados)

                    # Obter o ID do país
                    try:
                        if linha[6] == '' or linha[6] is None:
                            linha[6] = "999"
                        id_pais = obter_id_pais(conexao, linha[6])
                    except (Exception, psycopg2.Error) as error:
                        continue

                    # Obter o ID da faixa etária
                    if linha[10] == '' or linha[10] is None:
                        linha[10] = "0"
                    id_faixa_etaria = obter_id_faixa_etaria(conexao, linha[10])

                    # Inserir ou atualizar os dados no banco de dados

                    # Verificar se o registro já existe na tabela
                    query = "SELECT \"CODG_HASH_SOC\" FROM basespessoas.\"PES_SOCIOS\" WHERE \"NOME_SOCIO_SOC\" = %s AND \"NUMR_CNPJ_CPF_SOC\" = %s"
                    cursor.execute(query, (linha[2], linha[3]))
                    resultado = cursor.fetchone()

                    if resultado:
                        dados_originais = (linha[1], linha[2], linha[3], linha[6], id_pais, linha[10], id_faixa_etaria, hash_md5, numero_atualizacao, dados[2], dados[3])
                        # Registro já existe, verificar se o hash é diferente
                        if resultado[0] != dados[5]:
                            # Atualizar os dados na tabela
                            query = """UPDATE basespessoas.\"PES_SOCIOS\" 
                            SET \"INDR_TIPO_PESSOA_SOC\" = %s,
                                \"NOME_SOCIO_SOC\" = %s,
                                \"NUMR_CNPJ_CPF_SOC\" = %s, 
                                \"CODG_PAIS_SOC\" = %s,
                                \"PES_SOCIOS_ID_PAIS_fkey\" = %s,
                                \"INDR_FAIXA_ETARIA_SOC\" = %s, 
                                \"PES_SOCIOS_ID_FAIXA_ETARIA_fkey\" = %s,
                                \"CODG_HASH_SOC\" = %s,
                                \"NUMR_ATUALIZACAO_SOC\" = %s 
                            WHERE \"NOME_SOCIO_SOC\" = %s AND \"NUMR_CNPJ_CPF_SOC\" = %s"""

                            cursor.execute(query, (linha[1], linha[2], linha[3], linha[6], id_pais, linha[10], id_faixa_etaria, hash_md5, numero_atualizacao, linha[2], linha[3],))
                            registros_gravados += 1

                            # Inserir o log no arquivo de texto
                            inserir_log("SOCIO", numero_atualizacao, arquivo, data_e_hora_atuais, dados_originais, linha, "U")

                    else:
                        # Registro não existe, inserir na tabela
                        query = """INSERT INTO basespessoas.\"PES_SOCIOS\" 
                        (\"INDR_TIPO_PESSOA_SOC\", 
                            \"NOME_SOCIO_SOC\",
                            \"NUMR_CNPJ_CPF_SOC\",
                            \"CODG_PAIS_SOC\",
                            \"PES_SOCIOS_ID_PAIS_fkey\",
                            \"INDR_FAIXA_ETARIA_SOC\",
                            \"PES_SOCIOS_ID_FAIXA_ETARIA_fkey\",
                            \"CODG_HASH_SOC\",
                            \"NUMR_ATUALIZACAO_SOC\")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

                        cursor.execute(query, (linha[1], linha[2], linha[3], linha[6], id_pais, linha[10], id_faixa_etaria, hash_md5, numero_atualizacao,))
                        registros_gravados += 1

                        # Inserir o log no arquivo de texto
                        inserir_log("SOCIO", numero_atualizacao, arquivo, data_e_hora_atuais, "", linha, "I")

                    #(registros_gravados, resultado) = inserir_atualizar_socio(conexao, [linha[1], linha[2], linha[3], linha[6], id_pais, linha[10], id_faixa_etaria, hash_md5, numero_atualizacao], registros_gravados)

                    total_registros += 1

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
                            
                    #Atualizar a barra de progresso
                    #progresso = registros_lidos / total_registros * 100
                    #print(f"Progresso: {progresso:.2f}%")

                conexao.commit()

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print("Erro: ", e, "Tipo: ", exc_type, "Nome: ",fname, "Linha: ", exc_tb.tb_lineno)
                print("Registro: ", linha)
                wait = input("Pressione Enter para continuar...")
                pass

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("Erro: ", e, "Tipo: ", exc_type, "Nome: ",fname, "Linha: ", exc_tb.tb_lineno)
        pass

    # Fechar a conexão com o banco de dados
    finally:
        conexao.commit()
        cursor.close()

    # Exibir o resultado
    print(f"Registros lidos: {registros_lidos}")
    print(f"Registros gravados: {registros_gravados}")

def main():
    # Pedir o caminho do arquivo CSV e o número da atualização
    caminho = input("Digite o caminho do arquivo CSV: ")
    numero_atualizacao = input("Digite o número da atualização: ")

    # Chamar a função para inserir/atualizar os dados
    arquivo = "K3241.K03200Y1.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y1.D40113.SOCIOCSV ---------------------")
    inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y2.D40113.SOCIOCSV"
    print("------------------ K3242.K03200Y1.D40113.SOCIOCSV ---------------------")
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

if __name__ == '__main__':
    main()