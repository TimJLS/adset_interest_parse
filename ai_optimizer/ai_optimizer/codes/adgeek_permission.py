#!/usr/bin/env python
# coding: utf-8

# In[ ]:


###FB
ADGEEK_FACEBOOK_API_ID = '958842090856883'
ADGEEK_FACEBOOK_APP_SECRET = 'a952f55afca38572cea2994d440d674b'
ADGEEK_FACEBOOK_ACCESS_TOKEN = 'EAANoD9I4obMBALrHTgMWgRujnWcZA3ZB823phs6ynDDtQxnzIZASyRQZCHfr5soXBZA7NM9Dc4j9O8FtnlIzxiPCsYt4tmPQ6ZAT3yJLPuYQqjnWZBWX5dsOVzNhEqsHYj1jVJ3RAVVueW7RSxRDbNXKvK3W23dcAjNMjxIjQGIOgZDZD'
#tim_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

from facebook_business.api import FacebookAdsApi

def init_facebook_api():
    FacebookAdsApi.init(ADGEEK_FACEBOOK_API_ID, ADGEEK_FACEBOOK_APP_SECRET, ADGEEK_FACEBOOK_ACCESS_TOKEN)

##############################################################################

###GOOGLE
from googleads import adwords
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)

def init_google_api():
    return adwords_client

