import requests
import pandas as pd
from tqdm import tqdm
import pyodbc


indicator_id = '460'


def get(token: str, indicator: str, start_date: str, end_date: str):
    url = f'https://api.esios.ree.es/indicators/{indicator}?&start_date={start_date}&end_date={end_date}/'
    headers = {'Authorization': f'Token token="{token}"'}

    print(f"Requesting json data from {url}...")

    data = requests.get(url,
                        headers=headers
                        )
    response_code = data.status_code

    if data.status_code is not 200:
        print(f"Response code {response_code} obtained. Please enter token again")
        token = input()
        return get(token, indicator, start_date, end_date)

    print(f"Response code: {response_code}")
    json = data.json()
    print("json data obtained successfully")

    return json


def df(json):
    table = pd.DataFrame()

    print("Converting JSON to DataFrame...")
    for val in tqdm(json['indicator']['values']):
        single_df = pd.DataFrame({'datetime': val['datetime_utc'], 'value': val['value']}, index=[0])
        table = table.append(single_df, ignore_index=True)

    table.insert(0, 'name', json['indicator']['name'])
    table.insert(1, 'id', json['indicator']['id'])
    table.insert(4, 'update_timestamp', json['indicator']['values_updated_at'])

    table.rename(columns={'value': 'demand_forecast'}, inplace=True)
    print("Conversion completed")

    print("Saving csv at 'data' folder")
    table.to_csv("data/david_blanco_etl.csv", index=False)


def connect():
    print("Conecting to DB. Please insert password:")
    password = input()
    try:
        driver = '{ODBC Driver 17 for SQL Server}'
        conn = pyodbc.connect(
            f'''DRIVER={driver}; SERVER=localhost; Database=mock_db; UID=sa; PWD={password};''')
        print("Successfully conected to DB")
        return conn

    except:
        print("wrong password")
        connect()


def create_table():
    conn = connect()
    cursor = conn.cursor()

    print("Creating Table")
    table = """
        CREATE TABLE demand_forecast_esios
        (
        id int PRIMARY KEY,
        name varchar(100),
        indicator_id int,
        demand_datetime varchar(100),
        demand_forecast int,
        update_timestamp varchar(100)
        )

    """

    print("table created successfully")
    cursor.execute(table)
    conn.commit()
    conn.close()


def load():
    create_table()

    df = pd.read_csv("/data/david_blanco_etl.csv")

    conn = connect()
    cursor = conn.cursor()
    print("Insertin values into DB")
    sql = "INSERT INTO demand_forecast_esios ([id],[name],[indicator_id], [demand_datetime], [demand_forecast], [update_timestamp]) VALUES (?,?,?,?,?,?)"
    sql_cols = df.columns.to_list()
    cursor.executemany(sql, df[sql_cols].values.tolist())
    conn.commit()