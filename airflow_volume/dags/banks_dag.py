import pandas as pd
import requests
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from config.constants import (
    MAIN_URL,
    QUERY_PARAMS,
    BANK_DICT,
    OFFERS_DICT
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def get_data_from_sravni(ti):
    logging.info('Make Request ...')

    response = requests.post(MAIN_URL, json=QUERY_PARAMS)
    data = response.json()

    logging.info('Get JSON -> Success!')

    ti.xcom_push(key='raw_data', value=data)
    logging.info('Raw data pushed to XCom.')


def preprocess_data(ti):
    def get_online_rate(item):
        try:
            values = item["rateTable"]["Онлайн"]["rows"][0]
            percentages = [
                float(percent.replace('*', '').strip('%'))
                for item in values
                for percent in item.split(' / ')
                if '%' in percent
            ]
            return max(percentages)
        except Exception:
            return 0
        
    logging.info('Fetching raw data from XCom...')

    # Pull data from XCom
    data = ti.xcom_pull(key='raw_data', task_ids='get_data_from_sravni')

    logging.info('Parsing JSON ...')

    # Parse response
    for item in data["items"]:
        BANK_DICT["name"].append(item["organization"]["name"]["short"])
        BANK_DICT["rate"].append(item["rate"])
        BANK_DICT["online_rate"].append(get_online_rate(item))
        BANK_DICT["term"].append(item["term"])
        BANK_DICT["amount_from"].append(item["amount"]["from"])
        BANK_DICT["amount_to"].append(item["amount"]["to"])
        BANK_DICT["offer_count"].append(item["groupCount"])

        if item["groupCount"] > 0:
            for group_item in item['groupItems']:
                OFFERS_DICT["bank_name"].append(item["organization"]["name"]["short"])
                OFFERS_DICT["rate"].append(group_item["rate"])
                OFFERS_DICT["online_rate"].append(get_online_rate(group_item))
                OFFERS_DICT["term"].append(group_item["term"])
                OFFERS_DICT["amount_from"].append(group_item["amount"]["from"])
                OFFERS_DICT["amount_to"].append(group_item["amount"]["to"])

    logging.info('Parse JSON -> Success!')

    # Create DataFrames
    bank_df = pd.DataFrame(BANK_DICT)
    offers_df = pd.DataFrame(OFFERS_DICT)

     # Process data
    bank_df['rate'] = bank_df['rate'].apply(lambda x: x.strip('до% '))
    bank_df['rate'] = bank_df['rate'].astype('float')
    bank_df['final_rate'] = bank_df[['rate', 'online_rate']].max(axis=1)

    offers_df['rate'] = offers_df['rate'].apply(lambda x: x.strip(' до%'))
    offers_df['rate'] = offers_df['rate'].astype('float')
    offers_df['final_rate'] = offers_df[['rate', 'online_rate']].max(axis=1)

    # Add date
    bank_df['date'] = pd.Timestamp.today()
    offers_df['date'] = pd.Timestamp.today()

    logging.info('Saving Tables ...')

    # Save processed data
    bank_df.to_csv('banks.csv', index=False)
    offers_df.to_csv('offers.csv', index=False)

    logging.info('Data Successfully Parsed and Saved!')



#
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'get_banks_data_sravni',
    default_args=default_args,
    description='A DAG to parse and preprocess banks data from Sravni using XCom',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Task 1: Parse data
    parse_data_task = PythonOperator(
        task_id='get_data_from_sravni',
        python_callable=get_data_from_sravni,
    )

    # Task 2: Preprocess data
    preprocess_data_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
    )

    # Task dependencies
    parse_data_task >> preprocess_data_task


