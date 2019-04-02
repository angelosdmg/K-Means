from HubspotFunctions import *
from HubspotTransformations import *
from DB_Functions import import_df_to_gold_db, truncate_hubspot_deal_stages_table
import pandas as pd
import copy



#To fix: Stop iterating deal_list twice
#Clear objects when usage is finished
#Documentation in Functions

#Getting Data from API
deal_list = []
get_deals_with_stage_history(deal_list)

#extract owners
deal_owners = {}
extract_deal_owners(deal_owners,deal_list)
df_owners = pd.DataFrame(deal_owners).T
df_owners['id'] = df_owners.index



#Transformations for stages

# in deals_final, we store the data after the transformations
deals_final = {}
extract_deal_stages(deal_list, deals_final)


# fixing datetimes
deals_fixed_datetime = {}
fix_datetime(deals_fixed_datetime, deals_final)

#Creating DataFrame for stages
df = pd.DataFrame(deals_fixed_datetime).T
df['id'] = df.index

#merging dataframes
final_df = pd.merge(df, df_owners, on='id', how='outer')

#fix date format of datetime
format_dataframe(final_df)
columns = ['Unknown', 'Unknown_2', 'Unknown_3', 'added_on_sequence', 'closed_lost', 'closed_won', 'contact_again_soon',
           'contacted', 'contract_sent', 'decision_maker_brought_in',
           'interested_in_sponsorship', 'mail_problems', 'need_update', 'never_opened_email', 'on_radar',
           'to_delete', 'white_label_solution', 'id', 'ownerid', 'stage_deleted']

final_df = final_df[columns]

#import to DB
truncate_hubspot_deal_stages_table()
import_df_to_gold_db('hubspot_stages','append', final_df)


