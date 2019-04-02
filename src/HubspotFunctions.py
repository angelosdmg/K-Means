
def get_deals_with_stage_history(deals):
    """
    Retrieves deals from husbspot with their dealstage history

    @:type deals: list
    @param deals: An empty list in which the deals are stored
    """

    get_all_deals_url = "https://api.hubapi.com/deals/v1/deal/paged?"

    extra_parameters_string = ""
    headers = {}
    parameter_dict = {'propertiesWithHistory': 'dealstage', 'properties': 'hubspot_owner_id'}
    search_key = 'deals'

    send_request_to_api(get_all_deals_url, extra_parameters_string, headers, deals, search_key,parameter_dict)


def get_deals_with_properties(deals):

    """
    Retrieves deals from husbspot with a number of properties

    @:type deals: list
    @param deals: An empty list in which the deals are stored
    """


    get_all_deals_url = "https://api.hubapi.com/deals/v1/deal/paged?"


    extra_parameters=['description', 'other_destinations', 'qualifies_for_s_rides', 'tribe', 'type_of_affiliate' , 'type_of_services', 'type_of_integration']
    extra_parameters_string ='&'.join([f"properties={x}" for x in extra_parameters])

    headers = {}
    parameter_dict = {}
    search_key = 'deals'

    send_request_to_api(get_all_deals_url, extra_parameters_string, headers, deals, search_key,parameter_dict)


def get_engagemets(engagements):
    """
    @:type engagements: list
    :param engagements: An empty list in which the engagements are stored
    """
    get_all_deals_url = "https://api.hubapi.com/engagements/v1/engagements/paged?"
    headers = {}
    parameter_dict = {}
    extra_parameters_string = ""
    search_key = 'results'

    send_request_to_api(get_all_deals_url, extra_parameters_string, headers, engagements, search_key, parameter_dict)


def send_request_to_api(url, extra_parameters_string, headers, results_list, search_key, parameter_dict ):
    """
    Formats the request string to the API, sends the request and stores the answer in  'results_list'

    @type url: str
    @param url: The url of hubspot api to send the get requests to
    @type extra_parameters_string: str
    @param extra_parameters_string: string with parameters that will be requested by the api. Mainly used for
                                    multiple properties request
    @type headers: dict
    @param headers: headers for get request
    @type results_list: list
    @param results_list: a list dictionaries where the results are stored
    @type search_key: The API answers a json file/dict. The data we want to retrieve are usually found under
                    'deals' or 'results' key
    @param search_key: str
    @type parameter_dict: dict
    @param parameter_dict: contains parameters and their values. Will be appended in the get request
    """

    import urllib
    import requests
    import json

    print('started retrieving data')
    #api key for our account
    hapikey = '7d4ff9cf-e6c9-4357-9541-9dec187d54f1'
    limit = 250

    parameter_dict['hapikey'] = hapikey
    parameter_dict['limit'] = limit

    #has_more is a value responded from Hubspot API. it informs us if the are more results
    has_more = True


    while has_more:
        #constructing the url for the request
        parameters = urllib.parse.urlencode(parameter_dict)
        if(extra_parameters_string != ""):
            get_url = url + extra_parameters_string + "&" + parameters
        else:
            get_url = url + parameters

        r = requests.get(url= get_url, headers = headers)

        #parsing json into dict
        response_dict = json.loads(r.text)

        #if has_more = false, the iteration stops
        has_more = response_dict['hasMore']

        results_list.extend(response_dict[search_key])

        #the offset parameter is added to parameter_dict. In the next iteration
        #this parameter will be added in the get_url and the api will answer
        #the 'next 250 deals'
        parameter_dict['offset']= response_dict['offset']
    print('number of elements retrieved', len(results_list))

