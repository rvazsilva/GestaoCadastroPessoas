import csv
import psycopg2

# Função para inserir os dados na tabela
def insert_data(table_name, codigo, denominacao, ColumnCodg, ColumnDesc):
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(database="BasesPessoas", user="postgres", password="postgres", host="localhost", port="5432")
        cursor = conn.cursor()

        # Verificar se o registro já existe na tabela
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE \"{ColumnCodg}\" = %s", (codigo,))
        count = cursor.fetchone()[0]

        if count == 0:
            # Inserir o registro na tabela
            cursor.execute(f"INSERT INTO {table_name} (\"{ColumnCodg}\", \"{ColumnDesc}\") VALUES (%s, %s)", (codigo, denominacao))
            conn.commit()
        else:
            print("Registro já existe na tabela: ", codigo, " ", denominacao)

    except (Exception, psycopg2.Error) as error:
        print("Erro ao inserir registro:", error, " ", codigo, " ", denominacao)

    finally:
        # Fechar a conexão com o banco de dados
        if conn:
            cursor.close()
            conn.close()

# Função para ler e processar o arquivo CSV
def process_csv_file(file_path, table_name, ColumnCodg, ColumnDesc):
    try:
        with open(file_path, newline='', encoding = "ISO 8859-1") as csvfile:
            reader = csv.reader(csvfile, delimiter=';', quotechar='"')

            # Contadores de registros lidos e gravados
            total_records = 0
            inserted_records = 0
            for row in reader:
                total_records += 1
                codigo = row[0]
                denominacao = row[1]

                # Inserir os dados na tabela
                insert_data(table_name, codigo, denominacao, ColumnCodg, ColumnDesc)
                inserted_records += 1

            print(f"Registros lidos: {total_records} ", file_path)
            print(f"Registros gravados: {inserted_records} ", table_name)

    except FileNotFoundError:
        print("Arquivo não encontrado: ", file_path)

    except IndexError:
        print("Arquivo CSV inválido.")

# Solicitar o caminho dos arquivos CSV
csv_path = input("Digite o caminho onde estão armazenados os arquivos CSV: ")

# Processar cada arquivo CSV e inserir os dados nas tabelas correspondentes
process_csv_file(csv_path + "/F.K03200$Z.D40113.PAISCSV", 'basespessoas.\"PES_PAIS\"', "CODG_PAIS_PAI", "NOME_PAIS_PAI")
process_csv_file(csv_path + "/F.K03200$Z.D40113.QUALSCSV", 'basespessoas.\"PES_QUALIF_SOCIO\"', 'CODG_QUALIF_SOC_QLF', 'DESC_QUALIF_SOC_QLF')
process_csv_file(csv_path + "/F.K03200$Z.D40113.NATJUCSV", 'basespessoas.\"PES_NATUR_JURID\"', 'CODG_NAT_JUR_NTJ', 'DESC_NAT_JUR_NTJ')
process_csv_file(csv_path + "/F.K03200$Z.D40113.MUNICCSV", 'basespessoas.\"PES_MUNIC\"', 'CODG_MUNIC_MUN', 'NOME_MUNIC_MUN')
process_csv_file(csv_path + "/F.K03200$Z.D40113.CNAECSV", 'basespessoas.\"PES_CNAE\"', 'CODG_CNAE_CNE', 'DESC_CNAE_CNE')
process_csv_file(csv_path + "/F.K03200$Z.D40113.MOTICSV", 'basespessoas.\"PES_MOT_SIT\"', 'CODG_MOT_SIT_MST', 'DESC_MOT_SIT_MST')
process_csv_file(csv_path + "/F.K03200$Z.D40113.FAIETCSV", 'basespessoas.\"PES_FAIXA_ETARIA\"', 'CODG_FAIXA_ETARIA_FET', 'DESC_FAIXA_ETARIA_FET')
process_csv_file(csv_path + "/F.K03200$Z.D40113.POREMPCSV", 'basespessoas.\"PES_PORTE_EMPRESA\"', 'CODG_PORTE_EMP_PEM', 'DESC_PORTE_EMP_PEM')
process_csv_file(csv_path + "/F.K03200$Z.D40113.SITCADCSV", 'basespessoas.\"PES_SIT_CADASTRAL\"', 'CODG_SIT_CADASTRAL_SCD', 'DESC_SIT_CADASTRAL_SCD')
process_csv_file(csv_path + "/F.K03200$Z.D40113.OPCADCSV", 'basespessoas.\"PES_OPCAO_TRIBUTARIA\"', 'CODG_OPCAO_TRIBUTARIA_OTR', 'DESC_OPCAO_TRIBUTARIA_OTR')