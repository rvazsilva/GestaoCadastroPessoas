import csv

caminho = input('Digite o caminho onde está armazenado o arquivo CSV: ')
nome_arquivo = input('Digite o nome do arquivo CSV: ')
nome = input('Digite o nome: ')
cpf_cnpj = input('Digite o CPF/CNPJ: ')

# Montar o caminho completo do arquivo CSV
caminho_completo = f'{caminho}/{nome_arquivo}'

try:
    with open(caminho_completo, 'r', encoding='iso-8859-1') as arquivo_csv:
        leitor_csv = csv.reader(arquivo_csv, delimiter=';', quotechar='"')

        contador_lidos = 0

        for linha in leitor_csv:
            contador_lidos += 1

            if linha [2] == nome and linha[3] == cpf_cnpj:
                print("Linha :", linha)
                print(f'Registros lidos: {contador_lidos}')
                wait = input('Pressione enter para continuar')

            if contador_lidos % 1000 == 0:
                print(f'Registros lidos: {contador_lidos}')

except Exception as e:
    print(f'Ocorreu um erro: {str(e)}')

finally:
     quit

