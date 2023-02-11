from datetime import datetime
import requests
import pandas as pd
import json
from sqlalchemy import create_engine

import config as cfg


# constants
# db
DATA_TIME = datetime.now()
HOST = cfg.HOST
PORT = cfg.PORT
DBNAME = cfg.DBNAME
USER = cfg.USER
PASSWORD = cfg.PASSWORD
DB_TABLE_NAME = cfg.DB_TABLE_NAME
# API
API_KEY = cfg.API_KEY


# func return dict from api url
def get_data_from_api(amount: int, from_val: str, to_val: str):
    payload = {}
    headers = {
        'apikey': API_KEY
    }
    # init API connection
    url = f'https://api.apilayer.com/exchangerates_data/convert?to={to_val}&from={from_val}&amount={amount}'
    with requests.request("GET", url, headers=headers, data=payload) as response:
        # check connection status
        print(f'status API connect : {response.status_code}')
        if response.status_code != 200:
            data = response.text
            print(f'data from API :\n{data}')
            pass
        else:
            data = response.json() # dict type
            json_object = json.dumps(data, indent=4)
            data = json.loads(json_object)
            df = pd.json_normalize(data)
            # print(df)
        return df


# simple import data ti DB with increment
def import_df_to_db(df: pd.DataFrame, table_name: str):
    engine = create_engine(f'postgresql://{USER}:{PASSWORD}@localhost:5432/{DBNAME}')
    df.to_sql(table_name, engine, if_exists='append')
    print(f'{DATA_TIME} >> Data import is successful')


# run
import_df_to_db(get_data_from_api(5, 'EUR', 'USD'), DB_TABLE_NAME)

