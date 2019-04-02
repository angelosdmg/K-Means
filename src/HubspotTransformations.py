
def extract_deal_owners(deal_owners,deal_list):
    """

    @:type deal_owners: list
    :param deal_owners: The deal owners will be stored in this list
    @:type deal_list: list
    :param deal_list: The deals list, retrieved from hubspot in HubspotFunctions
    """
    import copy
    for deal in deal_list:

        if 'hubspot_owner_id' in deal['properties']:
            temp = {}
            temp['ownerid'] = deal['properties']['hubspot_owner_id']['value']
            deal_owners[deal['dealId']] = copy.deepcopy(temp)
            temp.clear()



def fix_datetime(deals_fixed_datetime, deals):
    """
    Formats unix timestamps to strings representing a date

    :param deals_fixed_datetime: List of deals with datetimes fixed
    :param deals_final: The deals list, retrieved from hubspot in HubspotFunctions
    """
    from datetime import datetime
    import copy

    for k in deals.items():

        items = {}
        for v in k[1].items():
            ts = int(v[1]) / 1000
            items[v[0]] = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')

        deals_fixed_datetime[k[0]] = copy.deepcopy(items)
        items.clear()
    print('unix to dates: done')



def format_dataframe(df):
    import pandas as pd


    df['Unknown'] = pd.to_datetime(df['Unknown'], format="%Y/%m/%d")
    df['Unknown_2'] = pd.to_datetime(df['Unknown_2'], format="%Y/%m/%d")
    df['Unknown_3'] = pd.to_datetime(df['Unknown_3'], format="%Y/%m/%d")
    df['added_on_sequence'] = pd.to_datetime(df['added_on_sequence'], format="%Y/%m/%d")
    df['closed_lost'] = pd.to_datetime(df['closed_lost'], format="%Y/%m/%d")
    df['closed_won'] = pd.to_datetime(df['closed_won'], format="%Y/%m/%d")
    df['contact_again_soon'] = pd.to_datetime(df['contact_again_soon'], format="%Y/%m/%d")
    df['contacted'] = pd.to_datetime(df['contacted'], format="%Y/%m/%d")
    df['contract_sent'] = pd.to_datetime(df['contract_sent'], format="%Y/%m/%d")
    df['decision_maker_brought_in'] = pd.to_datetime(df['decision_maker_brought_in'], format="%Y/%m/%d")
    df['interested_in_sponsorship'] = pd.to_datetime(df['interested_in_sponsorship'], format="%Y/%m/%d")
    df['mail_problems'] = pd.to_datetime(df['mail_problems'], format="%Y/%m/%d")
    df['need_update'] = pd.to_datetime(df['need_update'], format="%Y/%m/%d")
    df['never_opened_email'] = pd.to_datetime(df['never_opened_email'], format="%Y/%m/%d")
    df['on_radar'] = pd.to_datetime(df['on_radar'], format="%Y/%m/%d")
    df['to_delete'] = pd.to_datetime(df['to_delete'], format="%Y/%m/%d")
    df['white_label_solution'] = pd.to_datetime(df['white_label_solution'], format="%Y/%m/%d")



#TODO: Fix this function. Too much duplicate code and iteration for no reason
def extract_deal_stages(deal_list, deals_final):
    print('starting transformations')
    import copy
#stage names retrieved from hubspot are not propertly formatted
#will use this names as reference
    stages_name_dict = {'8e70ec82-c539-4b3d-9122-8ed06c8ebbe5': 'need_update',
                    'presentationscheduled': 'contacted',
                    'appointmentscheduled': 'on_radar',
                    '0fcac7d2-02f4-4523-8af4-9078bfad2804': 'added_on_sequence',
                    '609eaf85-cb37-44ad-b42d-293854030f59': 'contact_again_soon',
                    '709951f3-efa3-48ee-ab94-7478433c9f52': 'never_opened_email',
                    '70bca228-647b-4bcd-b829-109aa3ef1a5e': 'white_label_solution',
                    '7507f16a-28c3-426f-ac95-6ffdf29fbcaf': 'to_delete',
                    '7aae4d9b-ad6c-4b9b-8479-c14da72095ea': 'mail_problems',
                    'd54bc8b4-be5b-4a2d-a0a9-2da00d9112ec': 'interested_in_sponsorship',
                    'closedwon': 'closed_won',
                    'decisionmakerboughtin': 'decision_maker_brought_in',
                    'contractsent': 'contract_sent',
                    'closedlost': 'closed_lost',
                    '218a2613-8919-4f7f-b89b-342f77ee0ed6': 'stage_deleted',
                    'b5b70224-f78e-4346-bada-eab022721235': 'Unknown',
                    'ca5b2f3c-b8f3-486d-a3bd-92623d97b9d8': 'Unknown_2',
                    '312668': 'Unknown_3'
                   }
    deal_stages = {}
    for key in deal_list:
        deal_stages[key['dealId']] = key['properties']['dealstage']['versions']

    #deal_stages contains unnecessary information. Well will transform it and use this one

    for key in deal_stages.items():
        #currently, key is a tuple {dealid, all_deal_stages}
        #stages, is a dict in which stages will be stored as {stage1: timestamp, stage2: timestamp}
        #for each dealid. in the end of the iteration, dealid and stages are appended to deals_final
        # and stages is cleared.
        stages = {}
        for stage_temp in key[1]:
            #Some deals may have moved back-and-forth, producing multiple deal stages
            #By this implementation, only the latest version of each stage is keeped
            if stage_temp['value'] in stages:
                if stage_temp['timestamp'] > stages[stage_temp['value']]:
                    stages[stages_name_dict[stage_temp['value']]] = stage_temp['timestamp']
            else:
                stages[stages_name_dict[stage_temp['value']]] = stage_temp['timestamp']

        deals_final[key[0]] = copy.deepcopy(stages)
        stages.clear()
    print('transformations done')



def unix_mil_to_datetime(x):
    """
    Formats unix timestamps to strings representing a date
    :param x:
    :return:
    """
    from datetime import datetime
    x = int(x) / 1000
    x = datetime.utcfromtimestamp(x).strftime('%Y-%m-%d')
    return x


def retrieve_value_from_properties(temp, k, value):
    if value in k['properties']:
        temp[value] = k['properties'][value]['value']
