"""
Thames Water Open Data
"""

import river_secrets
import requests
import json
import pandas as pd

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
        else:
            df = pd.DataFrame()
    else:
        raise Exception("Request failed with status code {0}, and error message: {1}".format(r.status_code, r.json()))
    return df

# https://prod-tw-opendata-app.uk-e1.cloudhub.io/data/STE/v1/DischargeCurrentStatus?col_1=ReceivingWaterCourse&operand_1=like&value_1=%25River%20Lee&col_2=Y&operand_2=gte&value_2=214710

status_endpoint = 'DischargeCurrentStatus'
status_params = {'col_1': 'ReceivingWaterCourse', 'operand_1':'like', 'value_1':'%River Lee', 'col_2':'Y', 'operand_2':'gte', 'value_2':'214710'} 

'''
df = api_call(status_endpoint, status_params)
select = df.loc[df['AlertStatus'] != 'Offline', 'LocationName']
print(select.to_list())
'''

# https://prod-tw-opendata-app.uk-e1.cloudhub.io/data/STE/v1/DischargeAlerts?limit=10&col_1=LocationName&operand_1=eq&value_1=Harpenden&col_2=DateTime&operand_2=gte&value_2=2024-01-01

sites = ['Barbers Lane', 'Harpenden', 'Kimpton Road (Vauxhall Rd)', 'Luton', 'New Bedford Road, Luton', 'Park Town South & West, Luton', 'Vauxhall Motors']

alerts_endpoint = 'DischargeAlerts'
alerts_params = {'col_1': 'LocationName', 'operand_1':'eq', 'value_1':sites[1], 'col_2':'DateTime', 'operand_2':'gte', 'value_2':'2024-01-01'}

df = api_call(alerts_endpoint, alerts_params)
print(df.head())
