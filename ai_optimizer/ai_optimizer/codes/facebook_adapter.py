#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# %load facebook_adapter.py
import datetime
import pandas as pd
import mysql_adactivity_save
import bid_operator
import json
import math
from facebook_datacollector import AdSets

DATADASE = "dev_facebook_test"
START_TIME = 'start_time'
STOP_TIME = 'stop_time'
AI_START_DATE = 'ai_start_date'
AI_STOP_DATE = 'ai_stop_date'
AD_ID = 'ad_id'
ADSET_ID = 'adset_id'
CAMPAIGN_ID = 'campaign_id'
CHARGE = 'charge'
TARGET = 'target'
BID_AMOUNT = 'bid_amount'
REQUEST_TIME = 'request_time'
TARGET_LEFT = 'target_left'

INIT_BID = 'init_bid'
LAST_BID = 'last_bid'
ADSET_PROGRESS = 'adset_progress'
CAMPAIGN_PROGRESS = 'campaign_progress'

class FacebookCampaignAdapter(object):
    def __init__(self, campaign_id):
        self.mydb = mysql_adactivity_save.connectDB( DATADASE )
        self.limit = 9000
        self.hour_per_day = 20
        self.campaign_id = campaign_id
        self.request_time = datetime.datetime.now()
        self.request_date = datetime.date.today()
        self.time_progress = ( self.request_time.hour + 1 ) / self.hour_per_day
        self.init_bid_dict = dict()
        self.last_bid_dict = dict()
        
        self.df_camp = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id={}".format( campaign_id ), con=self.mydb )
        try:
            self.campaign_days_left = ( self.df_camp[ AI_STOP_DATE ].iloc[0] - self.request_date ).days + 1
        except Exception as e:
            self.campaign_days_left = 1
        self.campaign_days = ( self.df_camp[ AI_STOP_DATE ].iloc[0] - self.df_camp[ AI_START_DATE ].iloc[0] ).days
        self.campaign_performance = self.df_camp[ TARGET ].sum()
        self.campaign_target = self.df_camp[ TARGET_LEFT ].iloc[0].astype(dtype=object)
        self.campaign_day_target = self.campaign_target / self.campaign_days_left
        self.campaign_progress = self.campaign_performance / self.campaign_day_target
        self.mydb.close()
            
    def get_df(self):
        return
    
    def get_bid(self):
        sql = "select * from (select * from adset_insights WHERE campaign_id = {} order by request_time desc) as a group by adset_id;".format( self.campaign_id )
        df_adset = pd.read_sql( sql, con=self.mydb )
        df_init_bid = pd.read_sql( "SELECT * FROM adset_initial_bid WHERE campaign_id={} ;".format( self.campaign_id ), con=self.mydb )

        self.get_adset_list()
        for adset in self.adset_list:
            init_bid = df_init_bid[BID_AMOUNT][df_init_bid.adset_id==adset].head(1).iloc[0].astype(dtype=object)
            last_bid = df_adset[BID_AMOUNT][df_adset.adset_id==adset].tail(1).iloc[0].astype(dtype=object)
            self.init_bid_dict.update({ adset: init_bid })
            self.last_bid_dict.update({ adset: last_bid })
        return
    
    def get_campaign_days_left(self):
        return campaign_days_left
        
    def get_campaign_days(self):
        return self.campaign_days
    
    def get_campaign_performance(self):
        return self.campaign_performance
    
    def get_campaign_target(self):
        return self.campaign_target
    
    def get_campaign_day_target(self):
        return self.campaign_day_target

    def get_campaign_progress(self):
        return self.campaign_progress
    
    def get_adset_list(self):
        return self.adset_list
    
    def retrieve_campaign_attribute(self):
        self.mydb = mysql_adactivity_save.connectDB( DATADASE )
        
        self.df_ad = pd.read_sql( "SELECT * FROM adset_insights where campaign_id={}".format( self.campaign_id ), con=self.mydb )
        self.adset_list = self.df_ad[ADSET_ID][( self.df_ad.request_time.dt.date == self.request_time.date())].unique().tolist()
        self.get_bid()
        
        self.mydb.close()
        return

class FacebookAdSetAdapter(FacebookCampaignAdapter):
    def __init__(self, adset_id, fb):
        self.mydb = mysql_adactivity_save.connectDB( DATADASE )
        self.adset_id = adset_id
        self.fb = fb

    def init_campaign(self, fb):
        self.time_progress = fb.time_progress
        self.limit = fb.limit
        self.hour_per_day = fb.hour_per_day
        self.request_time = fb.request_time
        self.df_ad = fb.df_ad
        self.init_bid_dict = fb.init_bid_dict
        self.last_bid_dict = fb.last_bid_dict
        self.campaign_days_left = fb.campaign_days_left
        self.campaign_days = fb.campaign_days
        self.campaign_performance = fb.campaign_performance
        self.campaign_target = fb.campaign_target
        self.campaign_day_target = fb.campaign_day_target
        self.campaign_progress = fb.campaign_progress
        return
    
    def get_campaign_id(self):
        self.campaign_id = self.df_ad[ CAMPAIGN_ID ].iloc[0].astype(dtype=object)
        return self.campaign_id
    
    def get_adset_day_target(self):
        adset_num = len( self.fb.adset_list )
        self.adset_day_target = self.campaign_day_target / adset_num
        return self.adset_day_target
    
    def get_adset_performance(self):
        try:
            self.adset_performance = self.df_ad[self.df_ad.adset_id==self.adset_id][[ TARGET ]].tail(1).iloc[0,0]
        except ValueError as e:
            print('[facebook_adapter.FacebookAdSetAdapter.get_adset_performance()]', e)
            self.adset_performance = 0
        if math.isnan(self.adset_performance):
            self.adset_performance = 0
        return self.adset_performance
    
    def get_bid(self):
        self.init_bid = self.init_bid_dict[self.adset_id]
        self.last_bid = self.last_bid_dict[self.adset_id]
        return
    
    def get_adset_time_target(self):
        self.adset_time_target = self.adset_day_target * self.time_progress
        return self.adset_time_target
    
    def get_adset_progress(self):
#         print(self.adset_performance, self.adset_time_target)
        self.adset_progress = self.adset_performance / self.adset_time_target
        return self.adset_progress
    
    def retrieve_adset_attribute(self):
        self.init_campaign(self.fb)
        self.get_campaign_id()
        self.get_adset_day_target()
        self.get_adset_performance()
        self.get_bid()
        self.get_adset_time_target()
        self.get_adset_progress()
        self.mydb.close()
        return {
            ADSET_ID:self.adset_id,
            INIT_BID:self.init_bid,
            LAST_BID:self.last_bid,
            ADSET_PROGRESS:self.adset_progress,
            CAMPAIGN_PROGRESS:self.campaign_progress
        }

def main():
    start_time = datetime.datetime.now()
    campaign_id_list = mysql_adactivity_save.get_campaign_target()['campaign_id'].unique()
    for campaign_id in campaign_id_list:
        print(campaign_id)
#         campaign_id = campaign_id.astype(dtype=object)
        result={ 'media': 'Facebook', 'campaign_id': campaign_id.astype(dtype=object), 'contents':[] }
        release_version_result = {  }
#         try:
        fb = FacebookCampaignAdapter( campaign_id )
        fb.get_df()
        fb.retrieve_campaign_attribute()
        adset_list = fb.get_adset_list()
        charge_type = fb.df_camp['charge_type'].iloc[0]
        for adset in adset_list:
            s = FacebookAdSetAdapter( adset, fb )
            status = s.retrieve_adset_attribute()
            media = result['media']
            bid = bid_operator.adjust(media, **status)
            result['contents'].append(bid)
#                 print(status)
            adset = AdSets(adset, charge_type)
            adset.get_adset_features()
            ad_list = adset.get_ads()
            bid['pred_cpc'] = bid.pop('bid')
            bid['pred_cpc'] = int( bid['pred_cpc'] )
            bid["status"] = adset.status
            for ad in ad_list:
                release_version_result.update( { ad: bid } )
# #                 print({ ad: bid })
            del s
#         print(type(result['campaign_id']), type(result['contents'][0]['adset_id']), type(result['contents'][0]['pred_cpc']))
#         print(release_version_result)
#         return
        mydict_json = json.dumps(result)
        release_json = json.dumps(release_version_result)
        mysql_adactivity_save.insert_result( campaign_id.astype(dtype=object), mydict_json, datetime.datetime.now() )
        mysql_adactivity_save.insert_release_result( campaign_id.astype(dtype=object), release_json, datetime.datetime.now() )
        del fb
#         except:
#             print('pass')
#             pass
        
#     campaign_id = 23843269222010540
#     result={ 'media': 'Facebook', 'campaign_id': campaign_id, 'contents':[] }
#     fb = FacebookCampaignAdapter( campaign_id )
#     fb.retrieve_campaign_attribute()
#     adset_list = fb.get_adset_list()
#     for adset in adset_list:
#         s = FacebookAdSetAdapter( adset, fb )
#         status = s.retrieve_adset_attribute()
#         media = result['media']
#         bid = bid_operator.adjust(media, **status)
#         result['contents'].append(bid)
#         del s
#     del fb
    
    print(datetime.datetime.now()-start_time)
    return
    
if __name__=='__main__':
    main()
    import gc
    gc.collect()
    


# In[ ]:




