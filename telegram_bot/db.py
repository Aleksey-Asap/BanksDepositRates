import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv

from constants import INTEREST_RATE_QUERY
from utils import is_docker_env

# загружаем переменные среды
load_dotenv()

# определяем параметры подлчения к postgres
user_name = os.getenv("POSTGRES_DB_USER")
pwd = os.getenv("POSTGRES_DB_PASSWORD")
db_name = os.getenv("POSTGRES_DB_NAME")
port = os.getenv("POSTGRES_DB_HOST_PORT")
host = 'localhost'

# название хоста будет другим если запускаем в Docker
if is_docker_env():
    host = 'postgres_db'
    port = os.getenv("POSTGRES_DB_CONTAINER_PORT")


def get_interest_rate_data():
    # создаем подключение к postgres
    engine = create_engine(f'postgresql://{user_name}:{pwd}@{host}:{port}/{db_name}')

    with engine.connect() as connection:
        result = connection.execute(text(INTEREST_RATE_QUERY))
        rows = result.fetchall()
        columns = result.keys()

    return [dict(zip(columns, row)) for row in rows]
