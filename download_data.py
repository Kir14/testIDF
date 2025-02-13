import requests
import time
from clickhouse_driver import Client

#coviguration
URL = 'http://api.open-notify.org/astros.json'
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_USER = 'userIDF'
CLICKHOUSE_PASSWORD = 'passIDF'
CLICKHOUSE_DATABASE = 'idf'
RAW_TABLE = 'RAW_TABLE'
PARSED_TABLE = 'PARSED_TABLE'
MV = 'MV'

client = Client (
    host = CLICKHOUSE_HOST,
    user = CLICKHOUSE_USER,
    password = CLICKHOUSE_PASSWORD,
    database = CLICKHOUSE_DATABASE
)

#Create tables
def create_tables():
    create_table_query = f"""
    create table if not exists {CLICKHOUSE_DATABASE}.{RAW_TABLE} (
        raw_data String,
        _inserted_at DateTime Default now()
    ) ENGINE = MergeTree()
    order by _inserted_at;
    """ 
    client.execute(create_table_query)
    create_table_query = f"""
    create table if not exists {CLICKHOUSE_DATABASE}.{PARSED_TABLE} (
    craft 			String,
    name 			String,
    _inserted_at 	DateTime
    ) ENGINE = ReplacingMergeTree(_inserted_at)
    order by (craft, name);
    """
    client.execute(create_table_query)
    create_table_query = f"""
    create materialized view if not exists {CLICKHOUSE_DATABASE}.{MV} to {CLICKHOUSE_DATABASE}.{PARSED_TABLE} as
    select 
        person.craft as craft
        , person.name as name
        , _inserted_at
    from {CLICKHOUSE_DATABASE}.{RAW_TABLE} rt
    ARRAY join JSONExtract(replaceAll(rt.raw_data, '''', '"'), 'people', 'Array(Tuple(craft String, name String))') as person
    """
    client.execute(create_table_query)

#download data
def download_data(url, max_retries=5):
    retry_delay = 2
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Check errors HTTP
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                print(f"Attempt {attempt + 1}: 429 Too Many Requests")
            elif response.status_code == 404:
                print(f"Attempt {attempt + 1}: 404 Not Found")
            elif response.status_code == 400:
                print(f"Attempt {attempt + 1}: 400 Bad Request")
            elif response.status_code == 500:
                print(f"Attempt {attempt + 1}: 500 Internal Server Error")
            elif response.status_code == 503:
                print(f"Attempt {attempt + 1}: 503 Service Unavailable")
        except Exception as e:  #handle any other exceptions
            print(f"An error occurred: {str(e)}")
        finally:
            time.sleep(retry_delay)
            retry_delay *= 2  # increase delay
    raise Exception(f"Failed after {max_retries} attempts")

def insert_data(data):
    insert_query = f"""
    insert into {CLICKHOUSE_DATABASE}.{RAW_TABLE} (raw_data)
    values (%(raw_data)s);
    """
    client.execute(insert_query, {"raw_data": data})
    print("Data inserted successfully")

if __name__ == '__main__':
    try:
        create_tables()
        data = download_data(URL)
        raw_json = str(data)
        insert_data(raw_json)
        optimize_query = f"OPTIMIZE TABLE {CLICKHOUSE_DATABASE}.{PARSED_TABLE} FINAL;"
        client.execute(optimize_query)
    except Exception as e:
        print(f"An error occurred: {e}")