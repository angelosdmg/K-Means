from pandas import DataFrame

from HubspotFunctions import *
from HubspotTransformations import *
from DB_Functions import import_df_to_gold_db, truncate_hubspot_deals_table
import pandas as pd
import copy


deal_list = []
get_deals_with_properties(deal_list)

properties_dict={}

for k in deal_list:
    current_id = k['dealId']
    temp = {}

    retrieve_value_from_properties(temp, k,'description')
    retrieve_value_from_properties(temp, k,'tribe')
    retrieve_value_from_properties(temp, k,'type_of_affiliate')
    retrieve_value_from_properties(temp, k,'type_of_services')
    retrieve_value_from_properties(temp, k,'type_of_integration')
    retrieve_value_from_properties(temp, k,'qualifies_for_s_rides')

    properties_dict[current_id] = copy.deepcopy(temp)

df: DataFrame = pd.DataFrame(properties_dict).T
df['id'] = df.index

# TODO: create function library for all string manipulation operations
df['description'] = df['description'].replace('German\n',' German', regex=True)

truncate_hubspot_deals_table()
import_df_to_gold_db('hubspot_deals','append', df)
