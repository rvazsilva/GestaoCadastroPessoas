import csv
import psycopg2

# Função para inserir os dados na tabela
def insert_data(table_name, codigo, denominacao):
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(database="nome_do_banco", user="usuario", password="senha", host="localhost", port="5432")
        cursor = conn.cursor()

        # Verificar se o registro já existe na tabela
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE CodigoDescritor_c = %s", (codigo,))
        count = cursor.fetchone()[0]

        if count == 0:
            # Inserir o registro na tabela
            cursor.execute(f"INSERT INTO {table_name} (CodigoDescritor_c, DenominacaoDescritor_c) VALUES (%s, %s)", (codigo, denominacao))
            conn.commit()
            print("Registro inserido com sucesso!")
        else:
            print("Registro já existe na tabela.")

    except (Exception, psycopg2.Error) as error:
        print("Erro ao inserir registro:", error)

    finally:
        # Fechar a conexão com o banco de dados
        if conn:
            cursor.close()
            conn.close()

# Função para ler e processar o arquivo CSV
def process_csv_file(file_path, table_name):
    try:
        with open(file_path, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=';', quotechar='"')

            # Contadores de registros lidos e gravados
            total_records = 0
            inserted_records = 0

            for row in reader:
                total_records += 1
                codigo = row[0]
                denominacao = row[1]

                # Inserir os dados na tabela
                insert_data(table_name, codigo, denominacao)
                inserted_records += 1

            print(f"Registros lidos: {total_records}")
            print(f"Registros gravados: {inserted_records}")

    except FileNotFoundError:
        print("Arquivo não encontrado.")

    except IndexError:
        print("Arquivo CSV inválido.")

# Solicitar o caminho dos arquivos CSV
csv_path = input("Digite o caminho onde estão armazenados os arquivos CSV: ")

# Processar cada arquivo CSV e inserir os dados nas tabelas correspondentes
process_csv_file(csv_path + "/F.K03200$Z.D40113.PAISCSV", "bases_pessoas.PES_PAIS")
process_csv_file(csv_path + "/F.K03200$Z.D40113.QUALSCSV", "bases_pessoas.PES_QUALIF_SOCIO")
process_csv_file(csv_path + "/F.K03200$Z.D40113.NATJUCSV", "bases_pessoas.PES_NATUR_JURID")
process_csv_file(csv_path + "/F.K03200$Z.D40113.MUNICCSV", "bases_pessoas.PES_MUNIC")
process_csv_file(csv_path + "/F.K03200$Z.D40113.CNAECSV", "bases_pessoas.PES_CNAE")
process_csv_file(csv_path + "/F.K03200$Z.D40113.MOTICSV", "bases_pessoas.PES_MOT_SIT")
process_csv_file(csv_path + "/F.K03200$Z.D40113.FAIETCSV", "bases_pessoas.PES_FAIXA_ETARIA")
process_csv_file(csv_path + "/F.K03200$Z.D40113.POREMPCSV", "bases_pessoas.PES_PORTE_EMPRESA")
process_csv_file(csv_path + "/F.K03200$Z.D40113.SITCADCSV", "bases_pessoas.PES_SIT_CADASTRAL")
process_csv_file(csv_path + "/F.K03200$Z.D40113.OPCADCSV", "bases_pessoas.PES_OPCAO_TRIBUTARIA")