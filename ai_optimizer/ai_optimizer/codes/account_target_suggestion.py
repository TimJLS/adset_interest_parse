#!/usr/bin/env python
# coding: utf-8

# In[41]:


# ref: https://developers.facebook.com/tools/explorer?method=GET&path=act_516689492098932%2Ftargetingsuggestions&version=v3.3&classic=0

from pathlib import Path
import datetime
import time

from facebook_business.api import FacebookAdsApi
import facebook_business.adobjects.adaccount as facebook_business_adaccount
import facebook_business.adobjects.adset as facebook_business_adset
import facebook_business.adobjects.campaign as facebook_business_campaign
import facebook_business.adobjects.adsinsights as facebook_business_adsinsights
import facebook_business.adobjects.adaccounttargetingunified as facebook_business_adaccounttarget

import facebook_datacollector as fb_collector
import mysql_adactivity_save as mysql_saver

ADSET_INSIGHT_QUERY_FIELD = {
    'account_id': facebook_business_adsinsights.AdsInsights.Field.account_id,
    'campaign_id': facebook_business_adsinsights.AdsInsights.Field.campaign_id
}


class Account_Suggestion_Handler(object):
    database_connector = mysql_saver.connectDB(mysql_saver.DATABASE)
    
    def __init__(self, account_id, suggestion_id, suggestion_name, suggestion_type, audience_size):
        self.account_id = account_id
        self.suggestion_id = suggestion_id
        self.suggestion_name = suggestion_name
        self.suggestion_type = suggestion_type
        self.audience_size = audience_size
        self.log_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
    def __str__(self):
        return str(self.__dict__)
    
    def save_into_database(self):
        cols = ', '.join(self.__dict__.keys())
        vals = self.__dict__.values()
        placeholders = ', '.join(['%s'] * len(self.__dict__))
        
        table_name = 'account_target_suggestion'
        stmt = "insert into `{table}` ({columns}) values ({values});".format(table = table_name, columns = cols , values = placeholders)
        
        mycursor = self.database_connector.cursor()
        mycursor.execute(stmt, list( vals) )
        self.database_connector.commit()
        
def get_account_id_by_adset(adset_id):
    this_adsets = facebook_business_adset.AdSet( adset_id ).remote_read(fields=["account_id"])
    account_id = this_adsets.get('account_id')
    return account_id

def get_account_id_by_campaign(campaign_id):
    this_campaign = facebook_business_campaign.Campaign( campaign_id ).remote_read(fields=["account_id"])
    account_id = this_campaign.get('account_id')
    return account_id
    
def get_account_suggestion(account_id_act):
    account = facebook_business_adaccount.AdAccount(account_id_act)
    targeting_suggestions = account.get_targeting_suggestions()
#     print(targeting_suggestions)
    return targeting_suggestions

def process_account_suggestion(account_id):
    account_id_act = 'act_' + str(account_id)
    account_suggestion_list = get_account_suggestion(account_id_act)
    print('[process_account_suggestion] len:', len(account_suggestion_list))
    
    for account_suggestion in account_suggestion_list:
        suggestion_id = account_suggestion.get(facebook_business_adaccounttarget.AdAccountTargetingUnified.Field.id)
        suggestion_name = account_suggestion.get(facebook_business_adaccounttarget.AdAccountTargetingUnified.Field.name)
        suggestion_type = account_suggestion.get(facebook_business_adaccounttarget.AdAccountTargetingUnified.Field.type)
        audience_size = account_suggestion.get(facebook_business_adaccounttarget.AdAccountTargetingUnified.Field.audience_size)
        
        account_suggestion_handler = Account_Suggestion_Handler(account_id, suggestion_id, suggestion_name, suggestion_type, audience_size)
        account_suggestion_handler.save_into_database()
#         print(account_suggestion_handler)
    

        
    
def save_suggestion_for_all_campaign():
    campaign_list =  mysql_saver.get_campaign_target().campaign_id.unique().tolist()
    print('[save_suggestion_for_all_campaign] current running campaign:', len(campaign_list), campaign_list )
    print()
    
    for campaign_id in campaign_list:
        account_id = get_account_id_by_campaign(campaign_id)
        print('[save_suggestion_for_all_campaign] account_id:', account_id, 'campaign_id:', campaign_id)
        process_account_suggestion(account_id)
        
def main():
    save_suggestion_for_all_campaign()
    

    
if __name__ == "__main__":
    main()


# In[ ]:




