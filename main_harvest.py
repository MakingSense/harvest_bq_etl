import pandas
import requests
import pandas as pd
from pandas.io.json import json_normalize
import sys
from datetime import datetime
import gc
import dateutil.parser

# Database connection details
# Detailed info on connecting to BigQuery http://cloud.google.com/bigquery/docs/pandas-gbq-migration
from pip._internal.utils.misc import tabulate

full_table_id = "ms_harvest_custom."
project_id = "ms-data-warehouse"
q_check_contents = """SELECT max(last_system_update_date) as last_system_update_date FROM  """
# Forecast App Access
access_hv_token = "************************************************************************"
hv_id = "789482"
url_api = "https://api.harvestapp.com/v2/"
schemas = ['clients', 'contacts', 'expense_categories', 'expenses', 'invoice_item_categories',
           'invoice_item_categories',
           'invoice_payments', 'invoices', 'project_tasks', 'project_users', 'projects', 'roles', 'tasks',
           'time_entries', 'user_project_tasks', 'user_projects',
           'user_roles', 'users']


def request_data(url, section):
    headers = {'Harvest-Account-ID': hv_id, 'Authorization': 'Bearer {}'.format(access_hv_token),
               'User-Agent': 'access-harvest-bigquery'}
    resp = requests.get(url + section, headers=headers)
    if resp.status_code != 200:
        print("Data Source Server Status Issue " + str(url) + " " + str(headers) + " " + str(resp))
    else:
        print("Server Status Shows New Update " + str(url))
        pass
    return resp


def request_composed_data(url):
    headers = {'Harvest-Account-ID': hv_id, 'Authorization': 'Bearer {}'.format(access_hv_token),
               'User-Agent': 'access-harvest-bigquery'}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print("Data Source Server Status Issue " + str(url) + " " + str(headers) + " " + str(resp))
    else:
        pass
    return resp


def construct_dataframe(r, section):
    json_result = r.json()[section]
    last_updated = dateutil.parser.parse(json_result[0]['updated_at'])

    df = pd.json_normalize(json_result, sep="_")
    df['last_system_update_date'] = last_updated
    df['insertion_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\nData For Insertion Constructed (" + section + ")" + str(df.count()))
    return df


def construct_invoices_dataframes(resp):
    #invoices
    json_result = resp.json()['invoices']
    #df1 = pd.json_normalize(json_result, sep='_', max_level=1)
    #insert_data(df1, 'invoices')
    #line_items
    #df2 = pd.DataFrame()
    df3 = pd.DataFrame()
    for i in range(len(json_result)):
        #df2 = df2.append(pd.json_normalize(json_result[i]['line_items'], sep='_', max_level=1))
        resp_payment = request_composed_data(url_api+'invoices/'+str(json_result[i]['id'])+'/payments')
        df3 = df3.append(pd.json_normalize(resp_payment.json()['invoice_payments'], sep='_', max_level=1))
    print(tabulate(df3))
    #insert_data(df3, 'invoice_payments')
    #insert_data(df2, 'invoice_line_items')

    return


def insert_data(df, section):
    df.to_gbq(full_table_id + section, project_id=project_id, if_exists='append')
    print("\nData Insertion Time: " + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return


def delete_data(section):
    pd.DataFrame().to_gbq(full_table_id + section, project_id=project_id, if_exists='replace')
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


def process_invoices():
    r = request_data(url_api, 'invoices')
    total = r.json()['total_entries']
    delete_data('invoice_payments')
    for page in range(total):
        construct_invoices_dataframes(r)
        r = request_data(url_api, 'invoices?page='+str(page+1))
    return


def process_table(schema):
    print("\nProcessing " + schema)
    r = request_data(url_api, schema)
    df = construct_dataframe(r, schema)
    # check_contents(r, df, q_check_contents, schema)
    insert_data(df, schema)
    return


print("\nStarting ingestion process")
# for schema in schemas:
#    process_table(schema)
#    gc.collect()
process_invoices()
sys.exit(0)
