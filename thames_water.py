import river_secrets
import requests
import json
import time
import pandas as pd
import pathlib

out_folder = 'results'
out_path = pathlib.Path(out_folder)
# make the output folder if needed
out_path.mkdir(parents=True, exist_ok=True)

def api_call(endpoint, params):
    api_root = 'https://prod-tw-opendata-app.uk-e1.cloudhub.io'
    api_resource = '/data/STE/v1/'
    
    # build the url
    url = api_root + api_resource + endpoint
    
    # send the request
    r = requests.get(url, headers={'client_id': river_secrets.tw_clientID, 'client_secret': river_secrets.tw_clientSecret}, params=params)
    print("Requesting from " + r.url)

    # check response status and use only valid requests
    if r.status_code == 200:
        response = r.json()
        if 'items' in response:
            #with open(out_filename, 'w', encoding='utf-8') as f:
            #    json.dump(response, f, ensure_ascii=False, indent=4)
            df = pd.json_normalize(response, 'items')
            print(f'df shape: {df.shape}')
        else:
            df = pd.DataFrame()
    else:
        raise Exception("Request failed with status code {0}, and error message: {1}".format(r.status_code, r.json()))
    return df

# https://prod-tw-opendata-app.uk-e1.cloudhub.io/data/STE/v1/DischargeCurrentStatus?col_1=ReceivingWaterCourse&operand_1=like&value_1=%25River%20Lee&col_2=Y&operand_2=gte&value_2=214710

status_endpoint = 'DischargeCurrentStatus'
status_params = {'col_1': 'ReceivingWaterCourse', 'operand_1':'like', 'value_1':'%River Lee', 'col_2':'Y', 'operand_2':'gte', 'value_2':'214710'} 

'''
# call to current status
df = api_call(status_endpoint, status_params)
select = df.loc[df['AlertStatus'] != 'Offline', 'LocationName']
print(select.to_list())
'''

# https://prod-tw-opendata-app.uk-e1.cloudhub.io/data/STE/v1/DischargeAlerts?limit=10&col_1=LocationName&operand_1=eq&value_1=Harpenden&col_2=DateTime&operand_2=gte&value_2=2024-01-01

def get_alert_events_for_site(site, q_start, q_end):
    alerts_endpoint = 'DischargeAlerts'
    alerts_params = {'limit': 500,
                        'col_1': 'LocationName', 'operand_1':'eq', 'value_1': site, 
                        'col_2':'DateTime', 'operand_2':'gte', 'value_2': q_start,
                        'col_3':'DateTime', 'operand_3':'lte', 'value_3': q_end}
    
    df = api_call(alerts_endpoint, alerts_params)
    
    events = []
    if not df.empty:
        # manipulate dataframe
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        df.set_index('DateTime', inplace=True)
        df.sort_index(inplace=True)
        # filter to AlertType column only
        alerts = df['AlertType']

        # save raw alerts data csv
        df.to_csv(out_path.joinpath(f'site {site}.csv'))

        # relate start and end alerts into events to get duration.
        start = None
        for ts, value in alerts.items():
            print(f'{value} - {ts}')
            if value not in ['Start', 'Stop']:
                print(f'## UNHANDLED VALUE: {value} at {ts}')
            
            if start == None:
                if value == 'Start':
                    print(f'# START - {ts}')
                    start = ts
            else:
                if value == 'Stop':
                    print(f'# STOP - {ts}')
                    event = {'location': site, 'start': start, 'stop': ts, 'duration': ts-start}
                    start = None
                    events.append(event)
    return events

# Edit here to change sites of interest and timeframe
sites = ['Barbers Lane', 'Harpenden', 'Kimpton Road (Vauxhall Rd)', 'Luton', 'New Bedford Road, Luton', 'Park Town South & West, Luton', 'Vauxhall Motors']
start_date = "2024-01-01"
end_date = "2024-02-24"
# ---

all_events = []
for s in sites:
    site_events = get_alert_events_for_site(s, start_date, end_date)
    all_events += site_events
    time.sleep(1)
#all_events = get_alert_events_for_site(sites[1])

events_df = pd.DataFrame(all_events)
events_df['duration hours'] = events_df['duration'].apply(lambda ts: ts.total_seconds() / 3600)
print(events_df.head())

events_df.to_csv(out_path.joinpath('all sites summary.csv'))

