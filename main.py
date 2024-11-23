import pandas as pd
import requests

from constants import (
    MAIN_URL,
    QUERY_PARAMS,
    BANK_DICT,
    OFFERS_DICT
)


# cписок необходимых функций для парсинга сайта
def get_online_rate(item):
    """
    Функция получающая онлайн ставку для банка (если имеется).
    Используем try/except для отлавливания ошибок. 
    Если у банка нет онлайн ставки поле "Онлайн", то записываем 0, иначе значение онлайн ставки.
    """
    try:
        values = item["rateTable"]["Онлайн"]["rows"][0]
        
        # объяснение смотри в файле experiment.ipynb
        percentages = [
            float(percent.replace('*', '').strip('%'))
            for item in values
            for percent in item.split(' / ')
            if '%' in percent
        ]
        return max(percentages)
    
    # except блок срабатывает когда возникает ошибка в try. Exception - это любая ошибка которая может возникнуть в try
    except Exception:
        return 0

# логирование (вывод в консоль)
print('Make Request ...')

# забираем данные с сайта, используя POST запрос
response = requests.post(MAIN_URL, json=QUERY_PARAMS)
data = response.json()

# логирование (вывод в консоль)
print('Get JSON -> Success!')

# логирование (вывод в консоль)
print('Parse JSON ...')

# распарсиваем полченный ответ от сервера с данными (JSON) 
for i, item in enumerate(data["items"], start=1):
    BANK_DICT["id"].append(i) # добавляем id/индекс банка, используя enumerate()
    BANK_DICT["name"].append(item["organization"]["name"]["short"])
    BANK_DICT["rate"].append(item["rate"])

    # добавляем ставку онлайн банков
    BANK_DICT["online_rate"].append(
        get_online_rate(item)
    )

    BANK_DICT["term"].append(item["term"])
    BANK_DICT["amount_from"].append(item["amount"]["from"])
    BANK_DICT["amount_to"].append(item["amount"]["to"])
    BANK_DICT["offer_count"].append(item["groupCount"])

    if item["groupCount"] > 0:
        for group_item in item['groupItems']:  #enumerate - каждому объекту задает индекс
            OFFERS_DICT["bank_id"].append(i)
            OFFERS_DICT["bank_name"].append(item["organization"]["name"]["short"])
            OFFERS_DICT["rate"].append(group_item["rate"])

            # добавляем ставку онлайн банков
            OFFERS_DICT["online_rate"].append(
                get_online_rate(group_item)
            )

            OFFERS_DICT["term"].append(group_item["term"])
            OFFERS_DICT["amount_from"].append(group_item["amount"]["from"])
            OFFERS_DICT["amount_to"].append(group_item["amount"]["to"])


# логирование (вывод в консоль)
print('Creating Tables ...')

# создаем таблицы для Банков и Предложений Банков
bank_df = pd.DataFrame(BANK_DICT)
offers_df = pd.DataFrame(OFFERS_DICT)

# добавим "final_rate", предварительно предобработаем "rate" -> убираем %
bank_df['rate'] = bank_df['rate'].apply(lambda x: x.strip('до% ')) # возвращает строку
bank_df['rate'] = bank_df['rate'].astype('float') # меняем тип данных на вещественный

# выбираем наибольшую ставку между "rate" и "online_rate"
bank_df['final_rate'] = bank_df[['rate', 'online_rate']].max(axis=1) # параметр axis=1 говорит что нужно найти самое большое значение по строчке

offers_df['rate'] = offers_df['rate'].apply(lambda x: x.strip(' до%'))
offers_df['rate'] = offers_df['rate'].astype('float')
offers_df['final_rate'] = offers_df[['rate', 'online_rate']].max(axis=1) # выбираем наибольшую ставку между "rate" и "online_rate"

# логирование (вывод в консоль)
print('Saving Tables ...')

# сохраняем в excel
bank_df.to_excel('banks.xlsx', index=False)
offers_df.to_excel('offers.xlsx', index=False)

# логирование (вывод в консоль)
print('Done!')