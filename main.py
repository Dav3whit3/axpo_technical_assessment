import david_blanco_etl as ETL

indicator_id = '460'
start_date = '2017-01-01T00:00:00Z'
end_date = '2018-12-31T24:00:00Z'
print("Please introduce your token")
token = input()

if __name__ == '__main__':
    json_data = ETL.get(token, indicator_id, start_date, end_date)
    dataframe = ETL.df(json_data)
    load = ETL.load()
