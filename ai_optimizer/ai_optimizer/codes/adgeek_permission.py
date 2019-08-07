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

from facebook_business.api import FacebookAdsApi

def init_facebook_api(account_id = None):
    if not account_id:
        FacebookAdsApi.init(ADGEEK_FACEBOOK_API_ID, ADGEEK_FACEBOOK_APP_SECRET, ADGEEK_FACEBOOK_ACCESS_TOKEN)
        return
    
    try:
        query_id = get_queryid_by_accountid(account_id)
        credential_id, credential_secret, credential_token = get_media_token_by_queryid(query_id)
        FacebookAdsApi.init(credential_id, credential_secret, credential_token)

    except:
        FacebookAdsApi.init(ADGEEK_FACEBOOK_API_ID, ADGEEK_FACEBOOK_APP_SECRET, ADGEEK_FACEBOOK_ACCESS_TOKEN)

##############################################################################

###GOOGLE
from googleads import adwords
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)

def init_google_api():
    return adwords_client


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
        content = json.loads(r.text)    
#         print('[get_media_token_by_queryid] content:', content )
        credential_id = content.get('credential_id')
        credential_secret = content.get('credential_secret')
        credential_token = content.get('credential_token')
#         print('[get_media_token_by_queryid] credential_id:', credential_id, ' credential_secret', credential_secret, ' credential_token', credential_token )
        return credential_id, credential_secret, credential_token
        


# In[3]:


if __name__=='__main__':
    init_facebook_api(350498128813378)


# In[ ]:




