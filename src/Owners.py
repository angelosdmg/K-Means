import requests
import json
import copy
import pandas as pd
from DB_Functions import import_df_to_gold_db, truncate_hubspot_owners_table

get_url = 'https://api.hubapi.com/owners/v2/owners?hapikey=7d4ff9cf-e6c9-4357-9541-9dec187d54f1'
r = requests.get(url= get_url)

#parsing json into dict
response_dict = json.loads(r.text)
owners = {}

for owner in response_dict:
    owner_temp = {}
    owner_temp['firstName'] = owner['firstName']
    owner_temp['lastName'] = owner['lastName']
    owner_temp['email'] = owner['email']
    owners[owner['ownerId']] = copy.deepcopy(owner_temp)
    owner_temp.clear

df = pd.DataFrame(owners).T
df['id'] = df.index
print(df)
truncate_hubspot_owners_table()
import_df_to_gold_db('hubspot_owners','append', df)
