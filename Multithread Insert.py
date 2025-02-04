import os
import pandas as pd
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor

# Função para processar e salvar os dados no banco de dados SQL Server
def process_file(file_path, engine, required_columns):
    try:
        # Carregar CSV com pandas
        df = pd.read_csv(file_path, sep=";", header=0, dtype=str)

        # Validar se todas as colunas esperadas existem no DataFrame
        existing_columns = set(df.columns)
        missing_columns = required_columns - existing_columns

        if missing_columns:
            print(f"Erro: Colunas ausentes no arquivo {file_path}: {missing_columns}")
            return  # Pula o arquivo problemático

        # Filtrando as colunas
        df = df[list(required_columns)]

        # Inserir diretamente no banco de dados em batch
        df.to_sql('view_cdr_data_mo_last_month_0724_arquivos_v2', con=engine, index=False, if_exists='append', chunksize=5000)

        print(f'Arquivo processado com sucesso: {file_path}')

    except Exception as e:
        print(f'Ocorreu um erro ao processar o arquivo {file_path}: {e}')

# Função principal para processar todos os arquivos
def process_files_and_save_to_db(local_dir, db_config):
    # Configuração da string de conexão usando SQLAlchemy
    connection_string = (
        f"mssql+pyodbc://{db_config['username']}:{db_config['password']}@{db_config['server']}/{db_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    )

    # Criação do engine SQLAlchemy
    engine = create_engine(connection_string)

    # Listar todos os arquivos CSV no diretório
    all_files = [os.path.join(local_dir, filename) for filename in os.listdir(local_dir) if filename.endswith(".csv")]

    # Colunas esperadas
    required_columns = {
        "idCDR", "brandID", "idSubscription", "subscriptionID", "msisdn",
        "startDate", "parseDay", "parseDate", "trafficUnits", "trafficUnitsRatedSession",
        "packageID", "ratingPackRef","destinationPattern"
    }

    # Processar arquivos em paralelo
    with ThreadPoolExecutor() as executor:
        executor.map(lambda file_path: process_file(file_path, engine, required_columns), all_files)

# Exemplo de chamada da função
local_dir = r"C:\Users\matheus.weinert\Desktop\CDR"  # Caminho para os arquivos CSV
db_config = {
    'server': '***********',
    'database': '***********',
    'username': '***********',
    'password': '***********'
}

process_files_and_save_to_db(local_dir, db_config)

 #voz
 #required_columns = {
 #       "idCDR", "brandID", "idSubscription", "subscriptionID", "msisdn", "originator", "destination",
 #       "startDate", "parseDay", "parseDate", "trafficUnits", "trafficUnitsRatedSession",
 #    "packageID", "ratingPackRef"
 #   }