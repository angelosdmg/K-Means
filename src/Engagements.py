import copy
import pandas as pd
from HubspotFunctions import get_engagemets
from HubspotTransformations import *
from DB_Functions import import_df_to_gold_db, truncate_hubspot_engagements_table

#list with all engagements.
engagements = []
get_engagemets(engagements)

#removing unnecessary data
engagements_dict = {}
for k in engagements:
    data_dict = {}
    data_dict['createdAt'] = k['engagement']['createdAt']
    data_dict['lastUpdated'] = k['engagement']['lastUpdated']
    data_dict['ownerId'] = k['engagement']['ownerId']
    data_dict['timestamp'] = k['engagement']['timestamp']
    data_dict['type'] = k['engagement']['type']
    if len(k['associations']['dealIds']) > 0:
        data_dict['dealid'] = k['associations']['dealIds'][0]

    current_id = k['engagement']['id']
    engagements_dict[current_id] = copy.deepcopy(data_dict)


#creating dataframe and transforming unix timestamps
df = pd.DataFrame(engagements_dict).T
df['id'] = df.index

#Change order of columns. This is crucial for the import in the DB
df = df[['createdAt', 'lastUpdated', 'ownerId', 'timestamp', 'type', 'id', 'dealid']]

df['createdAt'] = df['createdAt'].apply(unix_mil_to_datetime)
df['lastUpdated'] = df['lastUpdated'].apply(unix_mil_to_datetime)
df['timestamp'] = df['timestamp'].apply(unix_mil_to_datetime)
df['createdAt'] = pd.to_datetime(df['createdAt'], format="%Y/%m/%d")
df['lastUpdated'] = pd.to_datetime(df['lastUpdated'], format="%Y/%m/%d")
df['timestamp'] = pd.to_datetime(df['timestamp'], format="%Y/%m/%d")


truncate_hubspot_engagements_table()
import_df_to_gold_db('hubspot_engagements','append', df)

