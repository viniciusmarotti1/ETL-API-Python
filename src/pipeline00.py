import time
import os
import requests
import logging
import logfire
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, BitcoinPreco
from dotenv import load_dotenv
from logging import basicConfig, getLogger

logfire.configure()
basicConfig(handlers=[logfire.LogfireLoggingHandler()])
logger = getLogger(__name__)
logger.setLevel(logging.INFO)
logfire.instrument_requests()
logfire.instrument_sqlalchemy()

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def create_table():
    Base.metadata.create_all(engine)
    logger.info("Tabela criada/verificada com sucesso!")

def extract_bitcoin_data():
    url = "https://api.coinbase.com/v2/prices/spot"

    response = requests.get(url) 
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Erro na API: {response.status_code}")
        return None

def transform_bitcoin_data(json_data):
    value = float(json_data["data"]["amount"])
    crypto = json_data["data"]["base"]
    coin = json_data["data"]["currency"]
    timestamp = datetime.now()
    
    transform_data = {
        "valor": value,
        "criptomoeda": crypto,
        "moeda": coin,
        "timestamp": timestamp
    }
    return transform_data

def save_data_postgres(data):
    session = Session()
    novo_registro = BitcoinPreco(**data)
    session.add(novo_registro)
    session.commit()
    session.close()
    logger.info(f"[{data['timestamp']}] Save data on PostgreSQL!")

def pipeline_bitcoin():
    with logfire.span("Executando pipeline ETL Bitcoin"):
        
        with logfire.span("Extrair Dados da API Coinbase"):
            json_data = extract_bitcoin_data()
        
        if not json_data:
            logger.error("Falha na extração dos dados. Abortando pipeline.")
            return
        
        with logfire.span("Tratar Dados do Bitcoin"):
            transform_data = transform_bitcoin_data(json_data)
        
        with logfire.span("Salvar Dados no Postgres"):
            save_data_postgres(transform_data)

        logger.info(
            f"Pipeline finalizada com sucesso!"
        )


if __name__ == "__main__":
    create_table()
    print("Starting ETL wtih update every 15 seconds")
    
    while True:
        try:
            json_data = extract_bitcoin_data()
            if json_data:
                processed_data = transform_bitcoin_data(json_data)
                print("Processed data: ", processed_data)
                save_data_postgres(processed_data)
            time.sleep(15)
        except KeyboardInterrupt:
            print("\nProcesso interrompido pelo usuário. Finalizando...")
            break
        except Exception as e:
            print(f"Erro durante a execução: {e}")
            time.sleep(15)
