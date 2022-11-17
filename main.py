PROJECT_ID = "your_project_id"
DATASET_ID = "your_dataset_id"

# type in ['string', 'integer', 'float']
# examples
event_params_input = {
    'page_location': {'type': 'string', 'description': 'URL of the page'},
    'page_title': {'type': 'string', 'description': 'Title of the page'},
    'ga_session_id': {'type': 'integer', 'description': 'Session ID'}
}

# to be added
# user_properties_params_input = {
#     'score_bracket': {'type': 'string', 'description': 'URL of the page'}
# }

project_dataset_id = PROJECT_ID + '.' + DATASET_ID + '.'
from datetime import datetime
from datetime import timedelta
import time
from google.cloud import bigquery
client = bigquery.Client(project=PROJECT_ID)

yesterday_yyyymmdd = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

# MOCKED DATE
# yesterday_yyyymmdd = '20221110'

raw_events_table_id = project_dataset_id + 'events_'+ yesterday_yyyymmdd
pivoted_events_table_id = project_dataset_id + 'pivoted_events_' + yesterday_yyyymmdd

def input_to_schema_fields(params):
  output = ''
  for i in params:
    field_name = i
    field_type = params[i]['type'].upper()
    if field_type == 'INTEGER':
      field_type='INT64'
    if field_type == 'FLOAT':
      field_type='FLOAT64'
    options = ' OPTIONS(description="'
    field_description = params[i]['description']
    output = output + field_name + " " + field_type + options + field_description + '"),'
  return output[:-1]

def input_to_query(params):
  output = ''
  for i in params:
    field_name = i
    field_type = params[i]['type'].lower()
    if field_type == 'integer':
      field_type='int'
    filler = ["(SELECT value.","_value FROM UNNEST(event_params) WHERE key = '", "') AS ", ","]
    output = output + filler[0] + field_type + filler[1] + field_name + filler[2] + field_name + filler[3]
  return output[:-1]

event_params_field = input_to_schema_fields(event_params_input)
event_params_query = input_to_query(event_params_input)

event_params_to_pivot = list(event_params_input.keys())
    

sql_query_drop_pivoted_table = '''
  DROP TABLE `'''+ pivoted_events_table_id + '''`'''

sql_query_create_pivoted_table = '''
  CREATE TABLE `'''+ pivoted_events_table_id + '''`
  (
    event_date STRING OPTIONS(description=""),
    event_timestamp INT64 OPTIONS(description=""),
    event_name STRING OPTIONS(description=""), ''' + event_params_field + ''',
    event_previous_timestamp INT64 OPTIONS(description=""),
    event_value_in_usd FLOAT64 OPTIONS(description=""),
    event_bundle_sequence_id INT64 OPTIONS(description=""),
    event_server_timestamp_offset INT64 OPTIONS(description=""),
    user_id STRING OPTIONS(description=""),
    user_pseudo_id STRING OPTIONS(description="_ga cookie for Web, firebase_app_instance_id for Apps"),
    privacy_info__analytics_storage STRING OPTIONS(description=""),
    privacy_info__ads_storage STRING OPTIONS(description=""),
    privacy_info__uses_transient_token STRING OPTIONS(description=""),

    user_first_touch_timestamp INT64 OPTIONS(description=""),
    user_ltv__revenue FLOAT64 OPTIONS(description=""),
    user_ltv__currency STRING OPTIONS(description=""),
    device__category STRING OPTIONS(description=""),
    device__mobile_brand_name STRING OPTIONS(description=""),
    device__mobile_model_name STRING OPTIONS(description=""),
    device__mobile_marketing_name STRING OPTIONS(description=""),
    device__mobile_os_hardware_model STRING OPTIONS(description=""),
    device__operating_system STRING OPTIONS(description=""),
    device__operating_system_version STRING OPTIONS(description=""),
    device__vendor_id STRING OPTIONS(description=""),
    device__advertising_id STRING OPTIONS(description=""),
    device__language STRING OPTIONS(description=""),
    device__is_limited_ad_tracking STRING OPTIONS(description=""),
    device__time_zone_offset_seconds INT64 OPTIONS(description=""),
    device__browser STRING OPTIONS(description=""),
    device__browser_version STRING OPTIONS(description=""),
    device__web_info__browser STRING OPTIONS(description=""),
    device__web_info__browser_version STRING OPTIONS(description=""),
    device__web_info__hostname STRING OPTIONS(description=""),
    geo__continent STRING OPTIONS(description=""),
    geo__country STRING OPTIONS(description=""),
    geo__region STRING OPTIONS(description=""),
    geo__city STRING OPTIONS(description=""),
    geo__sub_continent STRING OPTIONS(description=""),
    geo__metro STRING OPTIONS(description=""),
    app_info__id STRING OPTIONS(description=""),
    app_info__version STRING OPTIONS(description=""),
    app_info__install_store STRING OPTIONS(description=""),
    app_info__firebase_app_id STRING OPTIONS(description=""),
    app_info__install_source STRING OPTIONS(description=""),
    traffic_source__name STRING OPTIONS(description=""),
    traffic_source__medium STRING OPTIONS(description=""),
    traffic_source__source STRING OPTIONS(description=""),
    stream_id STRING OPTIONS(description=""),
    platform  STRING OPTIONS(description="WEB/IOS/ANDROID"),
    ecommerce__total_item_quantity INT64 OPTIONS(description=""),
    ecommerce__purchase_revenue_in_usd FLOAT64 OPTIONS(description=""),
    ecommerce__purchase_revenue FLOAT64 OPTIONS(description=""),
    ecommerce__refund_value_in_usd FLOAT64 OPTIONS(description=""),
    ecommerce__refund_value FLOAT64 OPTIONS(description=""),
    ecommerce__shipping_value_in_usd FLOAT64 OPTIONS(description=""),
    ecommerce__shipping_value FLOAT64 OPTIONS(description=""),
    ecommerce__tax_value_in_usd FLOAT64 OPTIONS(description=""),
    ecommerce__tax_value FLOAT64 OPTIONS(description=""),
    ecommerce__unique_items INT64 OPTIONS(description=""),
    ecommerce__transaction_id STRING OPTIONS(description="")
  ) '''


client.query(sql_query_drop_pivoted_table)
time.sleep(10)
client.query(sql_query_create_pivoted_table)

sql_query_insert_table = '''
  INSERT `''' + pivoted_events_table_id + '''`
  SELECT
    event_date,
    event_timestamp,
    event_name,
    ''' + event_params_query + ''',
    event_previous_timestamp,
    event_value_in_usd,
    event_bundle_sequence_id,
    event_server_timestamp_offset,
    user_id,
    user_pseudo_id,
    privacy_info.analytics_storage,
    privacy_info.ads_storage,
    privacy_info.uses_transient_token,

    user_first_touch_timestamp,
    user_ltv.revenue,
    user_ltv.currency,
    device.category,
    device.mobile_brand_name,
    device.mobile_model_name,
    device.mobile_marketing_name,
    device.mobile_os_hardware_model,
    device.operating_system,
    device.operating_system_version,
    device.vendor_id,
    device.advertising_id,
    device.language,
    device.is_limited_ad_tracking,
    device.time_zone_offset_seconds,
    device.browser,
    device.browser_version,
    device.web_info.browser,
    device.web_info.browser_version,
    device.web_info.hostname,
    geo.continent,
    geo.country,
    geo.region,
    geo.city,
    geo.sub_continent,
    geo.metro,
    app_info.id,
    app_info.version,
    app_info.install_store,
    app_info.firebase_app_id,
    app_info.install_source,
    traffic_source.name,
    traffic_source.medium,
    traffic_source.source,
    stream_id,
    platform,
    ecommerce.total_item_quantity,
    ecommerce.purchase_revenue_in_usd,
    ecommerce.purchase_revenue,
    ecommerce.refund_value_in_usd,
    ecommerce.refund_value,
    ecommerce.shipping_value_in_usd,
    ecommerce.shipping_value,
    ecommerce.tax_value_in_usd,
    ecommerce.tax_value,
    ecommerce.unique_items,
    ecommerce.transaction_id
  FROM `''' + raw_events_table_id + '''`'''
client.query(sql_query_insert_table)
