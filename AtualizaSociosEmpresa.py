import csv
import hashlib
import psycopg2
from datetime import datetime

# Função para calcular o hash MD5
def calcular_hash(dados):
    md5 = hashlib.md5()
    md5.update(dados.encode('utf-8'))
    return md5.hexdigest()

# Função para inserir um registro na tabela PES_SOCIOS_EMPRESA
def inserir_sociedade(cursor, id_empresa, id_socio, id_qualif_socio, id_qualif_repres, codg_qualific_socio, data_entrada_sociedade, cnpj_cpf_repres, nome_repres, codg_qualific_repres, hash_linha, numr_atualizacao):
    query = f"INSERT INTO basespessoas.\"PES_SOCIOS_EMPRESA\" (\"PES_SOCIOS_EMPRESA_ID_EMPRESA_fkey\", \"PES_SOCIOS_EMPRESA_ID_SOCIOS_fkey\", \"PES_SOCIOS_EMPRESA_ID_QUALIF_SOCIO_fkey\", \"PES_SOCIOS_EMPRESA_ID_QUALIF_REPRES_fkey\", \"CODG_QUALIFIC_SOCIO_SEM\", \"DATA_ENTRADA_SOCIEDADE_SEM\", \"NUMR_CNPJ_CPF_REPRES_SEM\", \"NOME_REPRES_SEM\", \"CODG_QUALIFIC_REPRES_SEM\", \"CODG_HASH_SEM\", \"NUMR_ATUALIZACAO_SEM\") VALUES ({id_empresa}, {id_socio}, {id_qualif_socio}, {id_qualif_repres}, '{codg_qualific_socio}', '{data_entrada_sociedade}', '{cnpj_cpf_repres}', '{nome_repres}', '{codg_qualific_repres}', '{hash_linha}', {numr_atualizacao})"
    cursor.execute(query)

# Função para atualizar um registro na tabela PES_SOCIOS_EMPRESA
def atualizar_sociedade(cursor, id_soc_emp, id_empresa, id_socio, id_qualif_socio, id_qualif_repres, codg_qualific_socio, data_entrada_sociedade, cnpj_cpf_repres, nome_repres, codg_qualific_repres, hash_linha, numr_atualizacao):
    query = f"""UPDATE basespessoas.\"PES_SOCIOS_EMPRESA\" SET \"PES_SOCIOS_EMPRESA_ID_EMPRESA_fkey\" = {id_empresa}, \"PES_SOCIOS_EMPRESA_ID_SOCIOS_fkey\" = {id_socio}, \"PES_SOCIOS_EMPRESA_ID_QUALIF_SOCIO_fkey\" = {id_qualif_socio}, \"PES_SOCIOS_EMPRESA_ID_QUALIF_REPRES_fkey\" = {id_qualif_repres}, \"CODG_QUALIFIC_SOCIO_SEM\" = '{codg_qualific_socio}', \"DATA_ENTRADA_SOCIEDADE_SEM\" = '{data_entrada_sociedade}', \"NUMR_CNPJ_CPF_REPRES_SEM\" = '{cnpj_cpf_repres}', \"NOME_REPRES_SEM\" = '{nome_repres}', \"CODG_QUALIFIC_REPRES_SEM\" = '{codg_qualific_repres}', \"CODG_HASH_SEM\" = '{hash_linha}', \"NUMR_ATUALIZACAO_SEM\" = {numr_atualizacao} WHERE \"PES_SOCIOS_EMPRESA_ID\" = {id_soc_emp}"""
    cursor.execute(query)

# Função para inserir um log no arquivo de texto
def inserir_log(numr_atualizacao, nome_arquivo, dados_originais, dados_atualizados):
    with open('/Users/rvazsilva/Temporario/Receita Federal/LogPessoa.txt', 'a') as arquivo:
        if arquivo.tell() == 0:
            arquivo.write("numr_atualizacao, arquivo, data_atualizacao, dados_originais, dados_atualizados\n")
        arquivo.write(f"{numr_atualizacao}, {nome_arquivo}, '', {dados_originais}, {dados_atualizados}\n")

def inserir_atualizar_dados (caminho, arquivo, numero_atualizacao):
# Função principal

    # Pedir caminho do arquivo CSV, nome do arquivo e número da atualização
    caminho = input("Digite o caminho onde o arquivo CSV está armazenado: ")
    nome_arquivo = input("Digite o nome do arquivo CSV: ")
    numero_atualizacao = input("Digite o número da atualização: ")

    # Abrir conexão com o banco de dados PostgreSQL
    conexao = psycopg2.connect(database="BasesPessoas", user="postgres", password="postagres", host="localhost", port="5432")
    cursor = conexao.cursor()

    # Abrir arquivo CSV
    with open(f"{caminho}/{nome_arquivo}", newline='', encoding='ISO 8859-1') as arquivo_csv:
        reader = csv.reader(arquivo_csv, delimiter=';', quotechar='"')

        # Variáveis para contar registros lidos e gravados
        contador_lidos = 0
        contador_gravados = 0
        data_e_hora_object = datetime.now()           
        data_e_hora_atuais = data_e_hora_object.strftime ('%Y-%m-%d %H:%M:%S')

        # Ler cada linha do arquivo CSV
        for linha in reader:
            contador_lidos += 1
            
            print ("Linha: ", linha)
            wait = input("Pressione Enter para continuar...")

            # Verificar se o registro existe na tabela PES_SOCIOS_EMPRESA
            query = f"""SELECT \"PES_ID_SOCIOS_EMPRESA\", \"CODG_HASH_SEM\" FROM basespessoas.\"PES_SOCIOS_EMPRESA\" 
                WHERE \"PES_SOCIOS_EMPRESA_ID_EMPRESA_fkey\" = 
                (SELECT \"PES_ID_EMPRESA\" FROM basespessoas.\"PES_EMPRESA\" 
                WHERE \"NUMR_CNPJ_BASICO_EMP\" = '{linha[0]}') AND 
                \"PES_SOCIOS_EMPRESA_ID_SOCIOS_fkey\" = 
                (SELECT \"PES_ID_SOCIOS\" FROM basespessoas.\"PES_SOCIOS\" 
                WHERE \"NOME_SOCIO_SOC\" = '{linha[2]}' AND \"NUMR_CNPJ_CPF_SOC\" = '{linha[3]}')"""
            cursor.execute(query)
            resultado = cursor.fetchone()

            print ("Resultado: ", resultado)
            wait = input("Pressione Enter para continuar...")

            if resultado is None:
                # Inserir novo registro na tabela PES_SOCIOS_EMPRESA
                hash_linha = calcular_hash(''.join(linha[5:10]))
                inserir_sociedade(cursor, linha[1], linha[3], linha[5], linha[6], linha[8], linha[9], linha[10], linha[11], linha[12], hash_linha, numero_atualizacao)
                inserir_log(numero_atualizacao, nome_arquivo, '', ''.join(linha[5:10]))
                contador_gravados += 1
            else:
                id_soc_emp, codg_hash_sem = resultado

                if codg_hash_sem != calcular_hash(''.join(linha[5:10])):
                    # Atualizar registro na tabela PES_SOCIOS_EMPRESA
                    atualizar_sociedade(cursor, id_soc_emp, linha[1], linha[3], linha[5], linha[6], linha[8], linha[9], linha[10], linha[11], linha[12], codg_hash_sem, numero_atualizacao)
                    inserir_log(numero_atualizacao, nome_arquivo, ','.join(linha[5:10]), ''.join(linha[5:10]))
                    contador_gravados += 1

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
 
        # Fazer commit das inserções restantes
        conexao.commit()

    # Fechar conexão com o banco de dados PostgreSQL
    cursor.close()
    conexao.close()

    # Exibir contador de registros lidos e gravados
    print(f"Registros lidos: {contador_lidos}")
    print(f"Registros gravados: {contador_gravados}")

def main():
    # Pedir o caminho do arquivo CSV e o número da atualização
    caminho = input("Digite o caminho do arquivo CSV: ")
    numero_atualizacao = input("Digite o número da atualização: ")

    # Chamar a função para inserir/atualizar os dados
    arquivo = "K3241.K03200Y1.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y1.D40113.SOCIOCSV ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y2.D40113.SOCIOCSV"
    print("------------------ K3242.K03200Y1.D40113.SOCIOCSV ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y3.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y3.D40113.SOCIOCSV ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y4.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y4.D40113.SOCIOCSV ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

    arquivo = "K3241.K03200Y5.D40113.SOCIOCSV"
    print("------------------ K3241.K03200Y5.D40113.SOCIOCSV ---------------------")
    #inserir_atualizar_dados(caminho, arquivo, numero_atualizacao)

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