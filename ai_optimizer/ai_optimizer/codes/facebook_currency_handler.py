#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pathlib import Path
import datetime
import time
import math

from facebook_business.api import FacebookAdsApi
import facebook_business.adobjects.adaccount as facebook_business_adaccount
import facebook_business.adobjects.adset as facebook_business_adset
import facebook_business.adobjects.campaign as facebook_business_campaign
import facebook_business.adobjects.adsinsights as facebook_business_adsinsights
import facebook_business.adobjects.adaccounttargetingunified as facebook_business_adaccounttarget
import database_controller
import adgeek_permission as permission


# In[ ]:


OFFSET_ONE = [
    'CLP','COP','CRC','HUF','ISK','IDR','JPY','KRW','PYG','TWD','VND']
OFFSET_A_HUNDRED = [
    'ARS',
    'AUD',
    'BDT',
    'BOB',
    'BRL',
    'GBP',
    'CAD',
    'CNY',
    'CZK',
    'DKK',
    'EGP',
    'EUR',
    'GTQ',
    'HNL',
    'HKD',
    'INR',
    'ILS',
    'KES',
    'MOP',
    'MYR',
    'MXN',
    'NZD',
    'NIO',
    'NGN',
    'NOK',
    'PKR',
    'PEN',
    'PHP',
    'PLN',
    'QAR',
    'RON',
    'RUB',
    'SAR',
    'SGD',
    'ZAR',
    'SEK',
    'CHF',
    'THB',
    'TRY',
    'AED',
    'USD',
    'UYU',
    'VEF'
]


# In[ ]:


def get_account_id_by_adset(adset_id):
    this_adsets = facebook_business_adset.AdSet( adset_id ).api_get(fields=["account_id"])
    account_id = this_adsets.get('account_id')
    return account_id

def get_account_id_by_campaign(campaign_id):
    this_campaign = facebook_business_campaign.Campaign( campaign_id ).api_get(fields=["account_id"])
    account_id = this_campaign.get('account_id')
    return account_id


def is_currency_existed(campaign_id):
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    df = database_fb.retrieve("facebook_campaign_currency", campaign_id, by_request_time=False)
    return not df.empty

def get_currency_from_database(campaign_id):
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    df = database_fb.retrieve("facebook_campaign_currency", campaign_id, by_request_time=False)

    for (idx, row,) in df.iterrows():
        currency = row["currency"]
    return currency


def insert_currency_into_database(campaign_id, currency):
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    df = database_fb.insert(
        "facebook_campaign_currency",
        {
            'campaign_id': campaign_id,
            'currency': currency,
        }
    )

def get_currency_by_campaign(campaign_id):
    
    currency = ''
    if is_currency_existed(campaign_id):
        currency = get_currency_from_database(campaign_id)
    else:
        account_id = get_account_id_by_campaign(campaign_id)
        
        account_id_act = 'act_' + str(account_id)
        currency = facebook_business_adaccount.AdAccount(account_id_act).api_get(fields=["currency"]).get('currency')
        insert_currency_into_database(campaign_id, currency)
    return currency

def get_proper_bid(campaign_id, init_bid):
    currency = get_currency_by_campaign(campaign_id)
    bid = init_bid
    if currency in OFFSET_A_HUNDRED:
        bid = math.ceil(init_bid*1.1)
    else:
        if init_bid > 100:
            bid = math.ceil(init_bid*1.1)
        else:
            bid = init_bid + 1
            
    return bid
    
    
def main():
    
    currency = get_currency_by_campaign(23843429637800443)
    print(currency)


# In[ ]:


# if __name__ == "__main__":
#     main()


# In[ ]:


# !jupyter nbconvert --to script facebook_currency_handler.ipynb


# In[ ]:




