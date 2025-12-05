import requests
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone
# import snowflake.connector
# from snowflake.connector.pandas_tools import write_pandas

# sams username 'al-7366'
load_dotenv(".env")

yesterday_noon = datetime.now(timezone.utc) - timedelta(days=1)
yesterday_noon = yesterday_noon.replace(hour=23, minute=0, second=0, microsecond=0)
formatted_time = yesterday_noon.isoformat()

# def snowflake_connect(schema: str) -> snowflake.connector:
#     '''Connects to the snowflake db'''

#     return snowflake.connector.connect(
#         user=os.environ['USERNAME'],
#         password=os.environ['PASSWORD'],
#         account=os.environ['ACCOUNT'],
#         warehouse=os.environ['WAREHOUSE'],
#         database=os.environ['DATABASE'],
#         schema=schema
#     )


def retrieve_access_token() -> str:
    '''Retrieves access token from txt file'''
    with open("token.txt") as f:
        return f.read()


def retrieve_items(offset: int, token: str) -> list[dict]:
    '''Retrieves ebay listings from ebay's api'''

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'X-EBAY-C-MARKETPLACE-ID': 'EBAY_GB'
    }

    params = {
        # 'category_ids': '149372',
        'limit': 200,
        'offset': offset,
        # 'q': 'One Piece'
    }

    r = requests.get(
        'https://api.ebay.com/buy/browse/v1/item_summary/search',
        headers=headers,
        params=params
    )

    

    return r.json().get('itemSummaries')

def retrieve_sales(offset: int, token: str) -> list[dict]:
    '''Retrieves ebay listings from ebay's api'''

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'X-EBAY-C-MARKETPLACE-ID': 'EBAY_GB'
    }

    params = {
        # 'category_ids': '149372',
        'limit': 200,
        'offset': offset,
        # 'q': 'One Piece',
        'filter': f'lastSoldDate:[{formatted_time}]'
    }

    # r = requests.get(
    #     'https://api.ebay.com/buy/browse/v1/item_summary/search',
    #     headers=headers,
    #     params=params
    # )
    r = requests.get(
    'https://api.ebay.com/buy/marketplace_insights/v1/item_sales/search',
    headers=headers,
    params=params
    )
    print("Status code:", r.status_code)
    print("Response headers:", r.headers)
    print("Response text:", r.text) 

    return r
# .get('itemSales')



def find_all_listings(token: str) -> pd.DataFrame:
    '''Finds all the listings between 1 and 1000 by recalling retrieve items'''
    df = pd.DataFrame(retrieve_sales(0, token))
    for num in range(200, 400, 200):
        df2 = pd.DataFrame(retrieve_sales(num, token))
        df = pd.concat([df, df2], ignore_index=True)
    return df


# def format_df(df: pd.DataFrame) -> pd.DataFrame:
#     '''Clean df ready to be loaded into raw table in snowflake'''
#     column_titles = ['itemId', 'title',
#                      'price', 'itemHref', 'seller', 'condition', 'conditionId', 'thumbnailImages', 'buyingOptions', 'itemWebUrl', 'itemLocation', 'itemOriginDate', 'itemCreationDate']
#     df = df[column_titles]
#     df['thumbnailImages'] = df['thumbnailImages'].apply(
#         lambda x: x[0].get('imageUrl'))
#     df['seller_username'] = df['seller'].apply(lambda x: x['username'])
#     df['seller_feedback_score'] = df['seller'].apply(
#         lambda x: x['feedbackPercentage'])
#     df['price'] = df['price'].apply(
#         lambda x: x['value'])
#     df['postcode'] = df['itemLocation'].apply(
#         lambda x: None if 'postalCode' not in x.keys() else x['postalCode'])
#     df['fixed_price'] = df['buyingOptions'].apply(
#         lambda x: True if 'FIXED_PRICE' in x else False)
#     df['auction'] = df['buyingOptions'].apply(
#         lambda x: True if 'AUCTION' in x else False)
#     df['best_offer'] = df['buyingOptions'].apply(
#         lambda x: True if 'BEST_OFFER' in x else False)
#     df = df.drop(['buyingOptions', 'itemLocation', 'seller'], axis=1)
#     df.columns = [col.lower() for col in df.columns]
#     print(df)
#     df.to_csv('items.csv')
#     return df


# def load_into_db(connection: snowflake.connector, raw_df: pd.DataFrame, raw_table_name, raw_schema):
#     '''load data into snowflake database table'''

#     success, nchunks, nrows, _ = write_pandas(
#         conn=connection,
#         df=raw_df,
#         table_name=raw_table_name,
#         database=os.environ['DATABASE'],
#         schema=raw_schema,
#         on_error='continue'
#     )
#     print(success)
#     print(nchunks)
#     print(nrows)


if __name__ == '__main__':
    # table_name = os.environ['TABLE']
    # schema = os.environ['SCHEMA']
    # print(os.environ['SCHEMA'])
    # conn = snowflake_connect(schema)
    token = retrieve_access_token()
    df = find_all_listings(token)
    # print(retrieve_sales(0, token))
    # print(df.columns)
    # print(df.head())
    # formatted_df = format_df(df)
    # load_into_db(conn, formatted_df, table_name, schema)
    # conn.close()
