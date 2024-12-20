import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv

from constants import INTEREST_RATE_QUERY

# загружаем переменные среды
load_dotenv()

# определяем параметры подлчения к postgres
user_name = os.getenv("POSTGRES_DB_USER")
pwd = os.getenv("POSTGRES_DB_PASSWORD")
port = os.getenv("POSTGRES_DB_HOST_PORT")
db_name = os.getenv("POSTGRES_DB_NAME")
host = 'localhost'

# названия таблиц
banks_table_name = 'banks'
offers_table_name = 'offers'

# создаем подключение к postgres
engine = create_engine(f'postgresql://{user_name}:{pwd}@{host}:{port}/{db_name}')


def get_interest_rate_data():
    with engine.connect() as connection:
        result = connection.execute(text(INTEREST_RATE_QUERY))
        rows = result.fetchall()
        columns = result.keys()

    return [dict(zip(columns, row)) for row in rows]

