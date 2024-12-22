import time
import os
import requests
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, BitcoinPreco
from dotenv import load_dotenv

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
    print("Create table with successful!")

def extract_bitcoin_data():
    url = "https://api.coinbase.com/v2/prices/spot"

    response = requests.get(url) 
    return response.json()

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
    print(f"[{data['timestamp']}] Save data on PostgreSQL!")

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
