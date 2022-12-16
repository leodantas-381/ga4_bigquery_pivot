import base64
from datetime import datetime
from datetime import timedelta
import time
import json
from google.cloud import bigquery
from sql_functions import input_to_schema_fields, input_to_query, get_sql

def pivot_table(body, event):
    pubsub_message = base64.b64decode(body['data']).decode('utf-8')
    pubsub_message_json = json.loads(pubsub_message)
    PROJECT_ID = pubsub_message_json['PROJECT_ID']
    DATASET_ID = pubsub_message_json['DATASET_ID']
    event_params_input = pubsub_message_json['event_params']
    user_properties_input = pubsub_message_json['user_properties'] 

    project_dataset_id = PROJECT_ID + '.' + DATASET_ID + '.'
    client = bigquery.Client(project=PROJECT_ID)
    yyyymmdd = ""
    if pubsub_message_json.get('yyyymmdd'):
      yyyymmdd = pubsub_message_json['yyyymmdd']
    if pubsub_message_json.get('intraday') == True:
      yyyymmdd = "intraday_" + (datetime.today()-timedelta(days=1)).strftime('%Y%m%d') 
    
    raw_events_table_id = project_dataset_id + 'events_'+ yyyymmdd
    pivoted_events_table_id = project_dataset_id + 'pivoted_events_' + yyyymmdd[-8:]

    event_params_field = input_to_schema_fields(event_params_input)
    event_params_query = input_to_query(event_params_input, "event_params")
    user_properties_field = input_to_schema_fields(user_properties_input)
    user_properties_query = input_to_query(user_properties_input, "user_properties")

    sqls =   get_sql(raw_events_table_id, pivoted_events_table_id, event_params_field, event_params_query, user_properties_field, user_properties_query)  
    client.query(sqls[0])
    time.sleep(10)
    client.query(sqls[1])
    time.sleep(10)
    client.query(sqls[2])