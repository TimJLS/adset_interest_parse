#!/usr/bin/env python
# coding: utf-8

# In[12]:


from pathlib import Path
import datetime
import time

from facebook_business.api import FacebookAdsApi
# from facebook_business.adobjects.adaccount import AdAccount
import facebook_business.adobjects.adset as facebook_business_adset
# from facebook_business.adobjects.ad import Ad
import facebook_business.adobjects.campaign as facebook_business_campaign
# from facebook_business.adobjects.adcreative import AdCreative
# from facebook_business.adobjects.adactivity import AdActivity
# from facebook_business.adobjects.insightsresult import InsightsResult
import facebook_business.adobjects.adsinsights as facebook_business_adsinsights

import facebook_datacollector as fb_collector
import mysql_adactivity_save as mysql_saver

my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBALrHTgMWgRujnWcZA3ZB823phs6ynDDtQxnzIZASyRQZCHfr5soXBZA7NM9Dc4j9O8FtnlIzxiPCsYt4tmPQ6ZAT3yJLPuYQqjnWZBWX5dsOVzNhEqsHYj1jVJ3RAVVueW7RSxRDbNXKvK3W23dcAjNMjxIjQGIOgZDZD'


class DatePreset:
    today = 'today'
    
ADSET_QUERY_FIELD = [  facebook_business_adset.AdSet.Field.id 
                     , facebook_business_adset.AdSet.Field.optimization_goal
                     , facebook_business_adset.AdSet.Field.bid_amount               
                     , facebook_business_adset.AdSet.Field.status ]


ADSET_INSIGHT_QUERY_FIELD = {
    'impressions': facebook_business_adsinsights.AdsInsights.Field.impressions,
    'spend': facebook_business_adsinsights.AdsInsights.Field.spend,
    'clicks': facebook_business_adsinsights.AdsInsights.Field.clicks,
}

class Adset_Log_Handler(object):
    database_connector = mysql_saver.connectDB(mysql_saver.DATABASE)
    table_name = 'adset_status_log'
    
    def __init__(self, campaign_id, adset_id, optimization_goal, bid_amount, status):
        self.campaign_id = campaign_id
        self.adset_id = adset_id
        self.optimization_goal = optimization_goal
        self.bid_amount = bid_amount
        self.status = status
        
        self.clicks = 0
        self.impressions = 0
        self.spend = 0
        
    def __str__(self):
        return str(self.__dict__)
    
    def set_insight(self):
        self.log_date = datetime.datetime.now().strftime("%Y-%m-%d")
        self.log_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        this_adsets = facebook_business_adset.AdSet( self.adset_id )
        insights = this_adsets.get_insights(
            params = {'date_preset': DatePreset.today },
            fields = ADSET_INSIGHT_QUERY_FIELD
        )

        if len(insights) == 1:
            this_adset_insight = insights[0]
#             print(this_adset_insight)
            self.clicks = this_adset_insight.get( facebook_business_adsinsights.AdsInsights.Field.clicks )
            self.impressions = this_adset_insight.get( facebook_business_adsinsights.AdsInsights.Field.impressions)
            self.spend = this_adset_insight.get( facebook_business_adsinsights.AdsInsights.Field.spend)
    
    def save_into_database(self):
        cols = ', '.join(self.__dict__.keys())
        vals = self.__dict__.values()
        placeholders = ', '.join(['%s'] * len(self.__dict__))
    
        stmt = "insert into `{table}` ({columns}) values ({values});".format(table = self.table_name, columns = cols , values = placeholders)
        
        mycursor = self.database_connector.cursor()
        mycursor.execute(stmt, list( vals) )
        self.database_connector.commit()
    
    def read_data(self):
        sql = "SELECT  HOUR(TIMEDIFF( max(log_datetime), min(log_datetime))) as hour_diff, (max(spend)-min(spend)) as spend_diff  FROM {} WHERE adset_id={} and log_date='{}' and status='ACTIVE' ".format(self.table_name, self.adset_id, self.log_date)
        
        print(sql)
        mycursor = self.database_connector.cursor()
        mycursor.execute(sql)
        hour_diff, spend_diff = mycursor.fetchone()
        print('result',hour_diff, spend_diff )
        
        
        
        
        
            
def get_adset_handler_by_campaign(campaign_id):
    camp = facebook_business_campaign.Campaign( campaign_id)
    adsets = camp.get_ad_sets(fields = ADSET_QUERY_FIELD)
    adset_handler_list = []
    
    for adset in adsets:
        adset_id = adset.get(facebook_business_adset.AdSet.Field.id)
        optimization_goal = adset.get(facebook_business_adset.AdSet.Field.optimization_goal)
        bid_amount = adset.get(facebook_business_adset.AdSet.Field.bid_amount)
        status = adset.get(facebook_business_adset.AdSet.Field.status)
        
        adset_handler = Adset_Log_Handler(campaign_id, adset_id, optimization_goal, bid_amount, status)
        adset_handler_list.append(adset_handler)
    return adset_handler_list
        

def process_campaign(campaign_id):
    adset_handler_list = get_adset_handler_by_campaign(campaign_id)
    print('campaign_id:', campaign_id, ' len(adset_handler_list):', len(adset_handler_list))
    
    for adset_handler in adset_handler_list:
        adset_handler.set_insight()
        adset_handler.save_into_database()
#         adset_handler.read_data()        

    
def main():

    campaign_list =  mysql_saver.get_campaign_target().campaign_id.unique().tolist()

    for campaign_id in campaign_list:
        start_time = time.time()
        process_campaign(campaign_id)
        print('campaign_id:', campaign_id, ' spend seconds:', time.time() - start_time)

    
if __name__ == "__main__":
    start_time = time.time()
    
    main()

#     process_campaign(23843368265910246)

    print('total campaign spend seconds:', time.time() - start_time)
    Adset_Log_Handler.database_connector.close()


# In[ ]:




