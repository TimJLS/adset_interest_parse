#!/usr/bin/env python
# coding: utf-8

# In[1]:


###FB
ADGEEK_FACEBOOK_API_ID = '958842090856883'
ADGEEK_FACEBOOK_APP_SECRET = 'a952f55afca38572cea2994d440d674b'
ADGEEK_FACEBOOK_ACCESS_TOKEN = 'EAANoD9I4obMBACygIE9jqmlaWeOW6tBma0oS6JbRpLgAvOYXpVi2XcXuasuwbBgqmaZBj5cP8MHE5WY2l9tAoi549eGZCP61mKr9BA8rZA6kxEW4ovX3KlbbrRGgt4RZC8MAi1UG0l0ZBUd0UBAhIPhzkZBi46ncuyCwkYPB7a6voVBZBTbEZAwH3azZA3Ph6g7aCOfxZCdDOp4AZDZD'
#ADGEEK_FACEBOOK_ACCESS_TOKEN = 'EAANoD9I4obMBALrHTgMWgRujnWcZA3ZB823phs6ynDDtQxnzIZASyRQZCHfr5soXBZA7NM9Dc4j9O8FtnlIzxiPCsYt4tmPQ6ZAT3yJLPuYQqjnWZBWX5dsOVzNhEqsHYj1jVJ3RAVVueW7RSxRDbNXKvK3W23dcAjNMjxIjQGIOgZDZD'

ACCOUNT_API_URL = 'http://mpc.adgeek.net/v2/accounts/'
ACCOUNT_TOEKN_API_URL = 'http://mpc.adgeek.net/v2/credentials/'

FACEBOOK_API_VERSION_URL = 'https://graph.facebook.com/v3.3/'

from facebook_business.session import FacebookSession
from facebook_business.api import FacebookAdsApi

def init_facebook_api(account_id = None):
    if not account_id:
        session = FacebookSession()
#         FacebookAdsApi(session, api_version="latest")
        FacebookAdsApi(session, api_version="3.3").init(ADGEEK_FACEBOOK_API_ID, ADGEEK_FACEBOOK_APP_SECRET, ADGEEK_FACEBOOK_ACCESS_TOKEN)
        return
    
    try:
        query_id = get_queryid_by_accountid(account_id)
        token_dic = get_media_token_by_queryid(query_id)
        credential_id = token_dic['credential_id']
        credential_secret = token_dic['credential_secret']
        credential_token  = token_dic['credential_token']
        session = FacebookSession()
#         FacebookAdsApi(session, api_version="latest")
        
        FacebookAdsApi(session, api_version="3.3").init(credential_id, credential_secret, credential_token)

    except:
        print('[init_facebook_api] error')
        session = FacebookSession()
#         FacebookAdsApi(session, api_version="latest")
        FacebookAdsApi(session, api_version="3.3").init(ADGEEK_FACEBOOK_API_ID, ADGEEK_FACEBOOK_APP_SECRET, ADGEEK_FACEBOOK_ACCESS_TOKEN)

##############################################################################

###GOOGLE
from googleads import adwords
from googleads import oauth2
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adgeek_adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)

def init_google_api(account_id = None):
    if not account_id:
        return adgeek_adwords_client
    
    try:
        query_id = get_queryid_by_accountid(account_id)
        token_dic = get_media_token_by_queryid(query_id)
        credential_id = token_dic['credential_id']
        credential_secret = token_dic['credential_secret']
        credential_developer_token  = token_dic['credential_developer_token']
        credential_refresh_token = token_dic['credential_refresh_token']
        
        oauth2_client = oauth2.GoogleRefreshTokenClient(credential_id, credential_secret, credential_refresh_token)
        my_adwords_client = adwords.AdWordsClient(credential_developer_token, oauth2_client, client_customer_id=account_id)
        return my_adwords_client


    except:
        print('[init_google_api] error')
        return adgeek_adwords_client


# In[2]:


import requests
import json 

def get_queryid_by_accountid(account_id):
#     print('[get_token_by_accountid] account_id:', account_id )
    
    my_params = {'filter[account_id]': account_id}
    r = requests.get(ACCOUNT_API_URL, params = my_params)

    if r.status_code == requests.codes.ok:
        content = json.loads(r.text)
#         print('[get_token_by_accountid] content:', content )
        
        data = content.get('_data')
#         print('[get_token_by_accountid] data:', data )
        
        if len(data) > 0:
            first_data = data[0]
#             print('first_data', first_data)
            query_id = first_data.get('credential_id')
#             print('[get_token_by_accountid] query_id:', query_id )
            return query_id

def get_media_token_by_queryid(query_id):
    request_url = ACCOUNT_TOEKN_API_URL + str(query_id)
#     print('[get_media_token_by_queryid] request_url:', request_url )
    
    r = requests.get(request_url)
    if r.status_code == requests.codes.ok:
        token_dic = {}
        content = json.loads(r.text)    
#         print('[get_media_token_by_queryid] content:', content )
        token_dic['credential_id'] = content.get('credential_id')#F #G
        token_dic['credential_secret'] = content.get('credential_secret')#F #G
        token_dic['credential_token'] = content.get('credential_token')#F
        token_dic['credential_developer_token'] = content.get('credential_developer_token')#G
        token_dic['credential_refresh_token'] = content.get('credential_refresh_token')#G
        token_dic['name'] = content.get('name')#G
        
        print('[get_media_token_by_queryid] token_dic', token_dic)
        return token_dic
        


# In[3]:


def get_access_token_by_account(account_id):
    query_id = get_queryid_by_accountid(account_id)
    token_dic = get_media_token_by_queryid(query_id)
    credential_token  = token_dic['credential_token']
    return credential_token
    
def get_access_name_by_account(account_id):
    query_id = get_queryid_by_accountid(account_id)
    token_dic = get_media_token_by_queryid(query_id)
    credential_token  = token_dic['name']
    return credential_token
    


# In[4]:


if __name__=='__main__':
    init_facebook_api(341659359840575)
#     init_google_api(6714857152)


# In[ ]:




