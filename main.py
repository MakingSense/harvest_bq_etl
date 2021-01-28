import requests
import pandas as pd
import sys
from pandas import json_normalize
from datetime import datetime
import gc
import dateutil.parser
import yaml


# Database connection details
# Detailed info on connecting to BigQuery http://cloud.google.com/bigquery/docs/pandas-gbq-migration
full_table_id = "ms_forecast_custom."
project_id = "ms-data-warehouse"
q_check_contents = """SELECT max(last_system_update_date) as last_system_update_date FROM  """
# Forecast App Access
access_hv_token = "2366395.pt.o0uOxxspBcIf8hhHVmY9s_OPp1XSwSBzZMyiDJ20Krqv8l_zU0vCHpCgOr1hsMLielP8OfNvzbmthdMsR80gHA"
hv_id = "681274"
url_api = "https://api.forecastapp.com/"
schemas = ['assignments', 'clients', 'projects', 'placeholders', 'people']


def request_data(url, section):
    headers = {'Forecast-Account-ID': hv_id, 'Authorization': 'Bearer {}'.format(access_hv_token),
               'User-Agent': 'access-harvest-bigquery'}
    resp = requests.get(url + section, headers=headers)
    if resp.status_code != 200:
        print("Data Source Server Status Issue " + str(url) + " " + str(headers) + " " + str(resp))
    else:
        print("Server Status Shows New Update "+ str(url))
        pass
    return resp


def construct_dataframe(r, section):
    json_result = r.json()[section]
    last_updated = dateutil.parser.parse(json_result[0]['updated_at'])

    df = json_normalize(json_result, sep="_")
    df['last_system_update_date'] = last_updated
    df['insertion_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\nData For Insertion Constructed (" + section + ")" + str(df.count()))
    return df


def insert_data(df, section):
    df.to_gbq(full_table_id + section, project_id=project_id, if_exists='append')
    print("\nData Insertion Time: " + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return


def check_contents(r, df, mq_check_contents, section):
    d = pd.read_gbq(mq_check_contents + full_table_id + section, project_id=project_id)
    # last system update record in the database
    last_system_update_date = d['last_system_update_date'][0]
    # last system update reported by the server response
    json_result = r.json()[section]
    last_updated = dateutil.parser.parse(json_result[0]['updated_at'])
    # check if the last_system_update_date entered in the database is less than the current date.
    # if it is not, do not insert the new data as you'll be adding duplicative records
    if last_system_update_date >= last_updated:
        print("\nData not inserted due to duplicative records ")
    else:
        print("\nData does not contain duplicate records ")
        insert_data(df)
    return


def process_table(schema):
    print("\nProcessing " + schema)
    r = request_data(url_api, schema)
    df = construct_dataframe(r, schema)
    check_contents(r, df, q_check_contents, schema)
    # insert_data(df, schema)
    return


print("\nStarting ingestion process")
for schema in schemas:
    process_table(schema)
    gc.collect()
sys.exit(0)
