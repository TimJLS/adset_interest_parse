#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
import mysql_adactivity_save as mysql_saver
import adgeek_permission as permission
# my_app_id = '958842090856883'
# my_app_secret = 'a952f55afca38572cea2994d440d674b'
# my_access_token = 'EAANoD9I4obMBACygIE9jqmlaWeOW6tBma0oS6JbRpLgAvOYXpVi2XcXuasuwbBgqmaZBj5cP8MHE5WY2l9tAoi549eGZCP61mKr9BA8rZA6kxEW4ovX3KlbbrRGgt4RZC8MAi1UG0l0ZBUd0UBAhIPhzkZBi46ncuyCwkYPB7a6voVBZBTbEZAwH3azZA3Ph6g7aCOfxZCdDOp4AZDZD'
# tim_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'
# # 'EAANoD9I4obMBAHukuWuiyeNxAPGEojU982JmGHJP1MsdM03H3gY3EVQj5G3gzZCq7KECX0lOi87ZCGKZA5hy1INxZBhD6azH8oYHICQ1BhZAojC50zjgUa54f9R2VInhLGpHGXl6F1VWltmRK6LeF5kRkDvZC4lkZCzSU4II1gJ1NoC0SaiS6D6piSp2rUTqPXTtASZBRfy5tbscOhfXDSxyai7IAaeCFb5xRBH4QsRm8wZDZD'

# FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)


def get_account_id_by_adset(adset_id):
    this_adsets = facebook_business_adset.AdSet( adset_id ).remote_read(fields=["account_id"])
    account_id = this_adsets.get('account_id')
    return account_id

def get_account_id_by_campaign(campaign_id):
    this_campaign = facebook_business_campaign.Campaign( campaign_id ).remote_read(fields=["account_id"])
    account_id = this_campaign.get('account_id')
    return account_id


def is_currency_existed(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = 'SELECT * FROM facebook_campaign_currency where campaign_id = {}'.format(campaign_id)
    my_cursor.execute(sql)
    result = my_cursor.fetchall()
    my_db.commit()
    my_db.close()
    return len(result) > 0

def get_currency_from_database(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = 'SELECT currency FROM facebook_campaign_currency where campaign_id = {}'.format(campaign_id)
    my_cursor.execute(sql)
    result = my_cursor.fetchone()
    currency = result[0]
    my_db.commit()
    my_db.close()
    return currency


def insert_currency_into_database(campaign_id, currency):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()    
    sql = "INSERT INTO facebook_campaign_currency ( campaign_id, currency ) VALUES ( %s, %s  )"
    val = ( campaign_id, currency )
    my_cursor.execute(sql, val)
    my_db.commit()
    my_db.close()

def get_currency_by_campaign(campaign_id):
    
    currency = ''
    if is_currency_existed(campaign_id):
        currency = get_currency_from_database(campaign_id)
    else:
        account_id = get_account_id_by_campaign(campaign_id)
        
        account_id_act = 'act_' + str(account_id)
        currency = facebook_business_adaccount.AdAccount(account_id_act).remote_read(fields=["currency"]).get('currency')
        insert_currency_into_database(campaign_id, currency)
    return currency

def get_proper_bid(campaign_id, init_bid):
    currency = get_currency_by_campaign(campaign_id)
    bid = init_bid
    if currency == 'USD':
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

    
if __name__ == "__main__":
    main()


# In[5]:





# In[ ]:




