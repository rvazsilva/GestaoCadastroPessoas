import sys, os
import csv
import hashlib
import psycopg2
from datetime import datetime

# Função para calcular o hash MD5
def calcular_hash(dados):
    md5 = hashlib.md5()
    md5.update(dados.encode('utf-8'))
    return md5.hexdigest()

# Função para buscar o ID de uma tabela com base em um valor específico
def buscar_id(cursor, id, tabela, coluna, valor, valor_padrao):
    cursor.execute(f"SELECT {id} FROM {tabela} WHERE {coluna} = %s", (valor,))
    resultado = cursor.fetchone()
    if resultado:
        return resultado[0]
    else:
        cursor.execute(f"INSERT INTO {tabela} ({coluna}) VALUES (%s) RETURNING {id}", (valor,))
        return cursor.fetchone()[0]
    
# Função para registrar o evento no arquivo de log
def registrar_evento(atualizacao, arquivo_csv, data_e_hora_atuais, dados_originais, dados_atualizados):
    with open('/Users/rvazsilva/Temporario/Receita Federal/LogPessoa.txt', 'a') as arquivo_log:
        if arquivo_log.tell() == 0:
            # Cabeçalho do arquivo de log
            arquivo_log.write('carga;numr_atualizacao;arquivo;data_atualizacao;dados_originais;dados_atualizados;oper\n')

        if dados_originais == '':
            # Inclusão
            linha = f'"EMPRES";{atualizacao};{arquivo_csv};{data_e_hora_atuais};"";{",".join(dados_atualizados)};"I"\n'
        else:
            # Alteração
            linha = f'"EMPRES";{atualizacao};{arquivo_csv};{data_e_hora_atuais};{",".join(dados_originais)};{",".join(dados_atualizados)};"U"\n'

        arquivo_log.write(linha)


# Função para inserir ou atualizar os dados na tabela
def inserir_atualizar_dados(caminho_arquivo, arquivo, atualizacao):
    try:
        arquivo_csv = caminho_arquivo + "/" + arquivo

        # Abrir o arquivo CSV
        with open(arquivo_csv, 'r', encoding='iso-8859-1') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';', quotechar='"')

            # Conectar ao banco de dados
            conn = psycopg2.connect(database="BasesPessoas", user="postgres", password="postgres", host="localhost", port="5432")
            cursor = conn.cursor()

            # Contadores de registros lidos e gravados
            contador_lidos = 0
            contador_gravados = 0
            contador_commit = 0
            data_e_hora_object = datetime.now()           
            data_e_hora_atuais = data_e_hora_object.strftime ('%Y-%m-%d %H:%M:%S')

            try:

                # Ler cada linha do arquivo CSV
                for linha in csv_reader:
                    contador_lidos += 1

                    # Extrair os valores da linha
                    cnpj = linha[0]
                    razao_social = linha[1]
                    natureza_juridica = linha[2]
                    qualif_representante = linha[3]
                    capital_social = float(linha[4].replace(",", "."))
                    porte_empresa = linha[5]
                    ente_federativo = linha[6]
                    indr_scp = 0
                    indr_simples_mei = 0

                    dados_atualizados = [linha[1], linha[2], linha[3], linha[4], linha[5], linha[6]]

                    # Concatenar os valores da linha para calcular o hash
                    dados = ''.join(linha)
                    hash_calculado = calcular_hash(dados)

                    # Verificar se o registro já existe na tabela
                    query = """SELECT 
                        \"PES_ID_EMPRESA\",
                        \"NOME_RAZAO_SOCIAL_EMP\",
                        \"CODG_NATUREZA_JURIDICA_EMP\",
                        \"CODG_QUALIF_REPRESENTANTE_EMP\",
                        \"VALR_CAPITAL_SOCIAL_EMP\",
                        \"CODG_PORTE_EMPRESA_EMP\",
                        \"CODG_ENTE_FEDERATIVO_EMP\",
                        \"CODG_HASH_EMP\" 
                    FROM basespessoas.\"PES_EMPRESA\" 
                    WHERE \"NUMR_CNPJ_BASICO_EMP\" = %s"""

                    cursor.execute(query, (cnpj,))
                    resultado = cursor.fetchone()

                    if resultado:
                        dados_originais = [resultado [1], resultado [2], resultado [3], resultado [4], resultado [5], resultado [6], resultado [7]]
                        id_empresa = resultado[0]
                        hash_bd = resultado[1]

                        # Comparar o hash calculado com o hash do banco de dados
                        if hash_calculado == hash_bd:
                            continue  # Próxima linha

                        # Atualizar os dados na tabela
                        query = """UPDATE basespessoas.\"PES_EMPRESA\"
                            SET \"NOME_RAZAO_SOCIAL_EMP\" = %s,
                            \"CODG_NATUREZA_JURIDICA_EMP\" = %s,
                            \"CODG_QUALIF_REPRESENTANTE_EMP\" = %s,
                            \"VALR_CAPITAL_SOCIAL_EMP\" = %s,
                            \"CODG_PORTE_EMPRESA_EMP\" = %s,
                            \"CODG_ENTE_FEDERATIVO_EMP\" = %s,
                            \"CODG_HASH_EMP\" = %s,
                            \"NUMR_ATUALIZACAO_EMP\" = %s
                        WHERE \"PES_ID_EMPRESA\" = %s"""
                        cursor.execute(query, (razao_social, natureza_juridica, qualif_representante, capital_social, porte_empresa, ente_federativo, hash_calculado, atualizacao, id_empresa,))

                        contador_gravados += 1

                        # Registrar inclusão no arquivo de log
                        registrar_evento(atualizacao, arquivo, data_e_hora_atuais, '', dados_atualizados)

                    else:
                        # Inserir os dados na tabela
                        id_natur_jurid = buscar_id(cursor, '\"PES_ID_NATUR_JURID\"', 'basespessoas.\"PES_NATUR_JURID\"', '\"CODG_NAT_JUR_NTJ\"', natureza_juridica, 'Codigo não cadastrado')
                        id_qualif_representante = buscar_id(cursor, '\"PES_ID_QUALIF_PESSOA\"', 'basespessoas.\"PES_QUALIF_PESSOA\"', '\"CODG_QUALIF_PESSOA_QLF\"', qualif_representante, 'Qualificação não cadastrada')
                        id_porte_empresa = buscar_id(cursor, '\"PES_ID_PORTE_EMP\"', 'basespessoas.\"PES_PORTE_EMPRESA\"', '\"CODG_PORTE_EMP_PEM\"', porte_empresa, 'Porte de empresa não cadastrada')

                        query = """INSERT INTO basespessoas.\"PES_EMPRESA\"
                            (\"NUMR_CNPJ_BASICO_EMP\",
                            \"NOME_RAZAO_SOCIAL_EMP\",
                            \"CODG_NATUREZA_JURIDICA_EMP\",
                            \"CODG_QUALIF_REPRESENTANTE_EMP\",
                            \"VALR_CAPITAL_SOCIAL_EMP\",
                            \"CODG_PORTE_EMPRESA_EMP\",
                            \"CODG_ENTE_FEDERATIVO_EMP\",
                            \"INDR_SCP_EMP\",
                            \"INDR_SIMPLES_MEI_EMP\",
                            \"CODG_HASH_EMP\",
                            \"NUMR_ATUALIZACAO_EMP\",
                            \"PES_EMPRESA_ID_NATUR_JURID_fkey\",
                            \"PES_EMPRESA_ID_QUALIF_REPRESENTANTE_fkey\",
                            \"PES_EMPRESA_ID_PORTE_EMPRESA_fkey\")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                        cursor.execute(query, (cnpj, razao_social, natureza_juridica, qualif_representante, capital_social, porte_empresa, ente_federativo, indr_scp, indr_simples_mei, hash_calculado, atualizacao, id_natur_jurid, id_qualif_representante, id_porte_empresa,))

                        contador_gravados += 1

                        # Registrar inclusão no arquivo de log
                        registrar_evento(atualizacao, arquivo, data_e_hora_atuais, '', dados_atualizados)

                    # Fazer commit a cada 1000 inserções
                    if contador_gravados != 0:
                        if contador_gravados % 1000 == 0:
                            data_e_hora_object = datetime.now()           
                            data_e_hora_atuais = data_e_hora_object.strftime ('%Y-%m-%d %H:%M:%S')
                            print(f'Registros lidos: {contador_lidos}')
                            print(f'Registros gravados: {contador_gravados}')
                            conn.commit()
                    else:
                        if contador_lidos % 1000 == 0:
                            data_e_hora_object = datetime.now()           
                            data_e_hora_atuais = data_e_hora_object.strftime ('%Y-%m-%d %H:%M:%S')
                            print(f'Registros lidos: {contador_lidos}')
                            print(f'Registros gravados: {contador_gravados}')
                            #conn.commit()

                # Fechar a conexão com o banco de dados
                print(f"Registros lidos: {contador_lidos}")
                print(f"Registros gravados: {contador_gravados}")
                conn.commit()
                cursor.close()
                conn.close()

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
        print("Erro ao inserir/atualizar dados, Type: ", exc_type, "Name: ", fname, "Linha: ", exc_tb.tb_lineno)

    #except (Exception, psycopg2.Error) as error:
    #    print("Erro ao inserir/atualizar dados:", error)

# Função principal
def main():
    # Pedir o caminho do arquivo CSV e o número da atualização
    caminho_arquivo = input("Digite o caminho do arquivo CSV: ")
    atualizacao = input("Digite o número da atualização: ")

    # Chamar a função para inserir/atualizar os dados
    arquivo = "K3241.K03200Y6.D40113.EMPRECSV"
    print("------------------ K3241.K03200Y6.D40113.EMPRECSV ---------------------")
    #inserir_atualizar_dados(caminho_arquivo, arquivo, atualizacao)

    arquivo = "K3241.K03200Y7.D40113.EMPRECSV"
    print("------------------ K3241.K03200Y7.D40113.EMPRECSV ---------------------")
    inserir_atualizar_dados(caminho_arquivo, arquivo, atualizacao)

    arquivo = "K3241.K03200Y8.D40113.EMPRECSV"
    print("------------------ K3241.K03200Y8.D40113.EMPRECSV ---------------------")
    inserir_atualizar_dados(caminho_arquivo, arquivo, atualizacao)

    arquivo = "K3241.K03200Y9.D40113.EMPRECSV"
    print("------------------ K3241.K03200Y9.D40113.EMPRECSV ---------------------")
    inserir_atualizar_dados(caminho_arquivo, arquivo, atualizacao)

    arquivo = "K3241.K03200Y0.D40113.EMPRECSV"
    print("------------------ K3241.K03200Y0.D40113.EMPRECSV ---------------------")
    inserir_atualizar_dados(caminho_arquivo, arquivo, atualizacao)

if __name__ == '__main__':
    main()
