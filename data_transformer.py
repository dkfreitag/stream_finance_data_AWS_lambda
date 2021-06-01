import json
import boto3
import yfinance as yf 
import pandas as pd

def lambda_handler(event, context):
    kinesis = boto3.client('kinesis', "us-east-2")
    df_stock = yf.download("FB SHOP BYND NFLX PINS SQ TTD OKTA SNAP DDOG",
                           period="1d",
                           interval="5m",
                           start="2021-05-11",
                           end="2021-05-12",
                           group_by="ticker")
    df_stock.reset_index(inplace=True)
    
    stock_list = ['FB', 'SHOP', 'BYND', 'NFLX', 'PINS', 'SQ', 'TTD', 'OKTA', 'SNAP', 'DDOG']
    dt_list = []
    high_list = []
    low_list = []
    name_list = []

    for stock in stock_list:
        for i in range(len(df_stock)):
            dt_list.append(str(df_stock[['Datetime', stock]]['Datetime'].iloc[i]))
            high_list.append(df_stock[['Datetime', stock]][stock]['High'].iloc[i])
            low_list.append(df_stock[['Datetime', stock]][stock]['Low'].iloc[i])
            name_list.append(stock)

    df_all_prices = pd.DataFrame(high_list, columns=['high'])
    df_all_prices['low'] = low_list
    df_all_prices['ts'] = dt_list
    df_all_prices['name'] = name_list
    
    for i in range(len(df_all_prices)):
        data = json.dumps(df_all_prices.iloc[i].to_dict())+"\n"
        print(data)
        kinesis.put_record(
                StreamName="STA9760S2021_stream1",
                Data=data,
                PartitionKey="partitionkey")
