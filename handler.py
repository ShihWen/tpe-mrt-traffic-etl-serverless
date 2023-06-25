import os
import pandas as pd
from helper import get_existing_file, get_traffic

pd.options.display.max_colwidth = 230

def mrt_traffic_file_list(event, context):
    
    event['mrt-traffic-file-dt'] = get_existing_file()
    
    return event


def mrt_traffic(event, context):
    try:        
        result_list = get_traffic(data_dt=event['mrt-traffic-file-dt'])     
        event['number_of_downloaded_files'] = len(result_list)
        event['date_of_downloaded_files'] = ','.join(result_list)
        event['company_type'] = 'traffic'
        event['company'] = os.environ['company']
        event['remaining_time_sec'] = context.get_remaining_time_in_millis()/1000
        
        return event
        
    except Exception as e:
        print(e)
        event['number_of_downloaded_files'] = 'function_error'
        event['date_of_downloaded_files'] = 'function_error'
        event['company_type'] = 'traffic'
        event['company'] = os.environ['company']
        event['remaining_time_sec'] = context.get_remaining_time_in_millis()/1000

        return event
