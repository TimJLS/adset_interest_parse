#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import datetime
import pandas as pd
import database_controller
import bid_operator
import json
import math
import facebook_datacollector as collector
import facebook_ai_behavior_log as ai_logger
import adgeek_permission as permission

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
ACTION = 'action'
BID_AMOUNT = 'bid_amount'
REQUEST_TIME = 'request_time'
TARGET_LEFT = 'target_left'

INIT_BID = 'init_bid'
LAST_BID = 'last_bid'
ADSET_PROGRESS = 'adset_progress'
CAMPAIGN_PROGRESS = 'campaign_progress'

class FacebookCampaignAdapter(object):
    def __init__(self, campaign_id, database_fb):
        self.database_fb = database_fb
        self.limit = 9000
        self.hour_per_day = 20
        self.campaign_id = campaign_id
        self.request_time = datetime.datetime.now()
        self.request_date = datetime.date.today()
        self.time_progress = ( self.request_time.hour + 1 ) / self.hour_per_day
        self.init_bid_dict = dict()
        self.last_bid_dict = dict()
        
        self.df_camp = self.database_fb.get_one_campaign(self.campaign_id)
        try:
            self.campaign_days_left = ( self.df_camp[ AI_STOP_DATE ].iloc[0] - self.request_date ).days + 1
        except Exception as e:
            self.campaign_days_left = 1
        
        self.campaign_days = ( self.df_camp[ AI_STOP_DATE ].iloc[0] - self.df_camp[ AI_START_DATE ].iloc[0] ).days
        self.campaign_performance = self.df_camp[ TARGET ].sum()
        self.campaign_target = self.df_camp[ TARGET_LEFT ].iloc[0].astype(dtype=object)
        self.campaign_day_target = self.campaign_target / self.campaign_days_left
        self.campaign_progress = self.campaign_performance / self.campaign_day_target
        self.campaign_progress = 1 if self.campaign_day_target <= 0 else self.campaign_progress
#         self.mydb.close()
        self.account_id = self.df_camp[ 'account_id' ].iloc[0].astype(dtype=object)
        permission.init_facebook_api(self.account_id)
            
    def get_df(self):
        return
    
    def get_bid(self):
#         df_init_bid = pd.read_sql( "SELECT * FROM adset_initial_bid WHERE campaign_id={} ;".format( self.campaign_id ), con=self.mydb )

        for adset in self.adset_list:           
            try:
                init_bid = self.database_fb.get_init_bid(adset)
                last_bid = self.database_fb.get_last_bid(adset)
#                 init_bid = df_init_bid[BID_AMOUNT][df_init_bid.adset_id==adset].head(1).iloc[0].astype(dtype=object)
#                 df_adset = pd.read_sql("select bid_amount from adset_metrics WHERE adset_id = {} order by request_time desc limit 1".format(adset), con=self.mydb)
#                 last_bid = df_adset.bid_amount.iloc[0].astype(dtype=object)
                self.init_bid_dict.update({ adset: init_bid })
                self.last_bid_dict.update({ adset: last_bid })
            except Exception as e:
                print('[facebook_adapter.get_bid]: lack init_bid or last_bid. ', e)
                pass
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
#         self.mydb = mysql_adactivity_save.connectDB( DATADASE )
        self.df_ad = self.database_fb.retrieve('table_insights', self.campaign_id)
        self.adset_list = [adset['adset_id'] for adset in self.df_ad.to_dict('records')]
#         self.df_ad = pd.read_sql( "SELECT * FROM adset_metrics where campaign_id={}".format( self.campaign_id ), con=self.mydb )
#         self.adset_list = self.df_ad[ADSET_ID][( self.df_ad.request_time.dt.date == self.request_time.date())].unique().tolist()
        self.get_bid()
        
#         self.mydb.close()
        return

class FacebookAdSetAdapter(FacebookCampaignAdapter):
    def __init__(self, adset_id, fb):
#         self.mydb = mysql_adactivity_save.connectDB( DATADASE )
        self.adset_id = int(adset_id)
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
        self.campaign_id = self.fb.campaign_id
        return self.campaign_id
    
    def get_adset_day_target(self):
        adset_num = len( self.fb.adset_list )
        self.adset_day_target = (self.campaign_day_target / adset_num) if adset_num != 0 else 1
        return self.adset_day_target
    
    def get_adset_performance(self):
        try:
            self.adset_performance = self.df_ad[self.df_ad.adset_id==self.adset_id][[ ACTION ]].tail(1).iloc[0,0]
        except Exception as e:
            print('[facebook_adapter.FacebookAdSetAdapter.get_adset_performance()]', e)
            self.adset_performance = 0
        if math.isnan(self.adset_performance):
            self.adset_performance = 0
        return self.adset_performance
    
    def get_bid(self):
        self.init_bid = self.init_bid_dict.get(self.adset_id)
        self.last_bid = self.last_bid_dict.get(self.adset_id)
        return
    
    def get_adset_time_target(self):
        self.adset_time_target = self.adset_day_target * self.time_progress
        return self.adset_time_target
    
    def get_adset_progress(self):
        self.adset_progress = self.adset_performance / self.adset_time_target
        self.adset_progress = 1 if self.adset_time_target <= 0 else self.adset_progress
        return self.adset_progress
    
    def retrieve_adset_attribute(self):
        self.init_campaign(self.fb)
        self.get_campaign_id()
        self.get_adset_day_target()
        self.get_adset_performance()
        self.get_bid()
        self.get_adset_time_target()
        self.get_adset_progress()
        return {
            ADSET_ID:self.adset_id,
            INIT_BID:self.init_bid,
            LAST_BID:self.last_bid,
            ADSET_PROGRESS:self.adset_progress,
            CAMPAIGN_PROGRESS:self.campaign_progress
        }

def main(campaign_id=None):
    start_time = datetime.datetime.now()
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    if campaign_id:
        print('==========[campaign_id]: {} =========='.format(campaign_id))
        result={ 'media': 'Facebook', 'campaign_id': campaign_id, 'contents':[] }
        collector_campaign = collector.Campaigns(campaign_id, database_fb=database_fb)
        fb = FacebookCampaignAdapter( campaign_id )
        fb.get_df()
        fb.retrieve_campaign_attribute()
        adset_list = collector_campaign.get_adsets_active()
        destination_type = collector_campaign.charge_type
        for adset_id in adset_list:
            s = FacebookAdSetAdapter( adset_id, fb )
            status = s.retrieve_adset_attribute()
            media = result['media']
            is_adjust_condition_sufficient=True
            for key, val in status.items():
                if val is None:
                    print('[facebook_adapter.main]: insufficient conditions to adjust bid, adset_id ', adset_id)
                    is_adjust_condition_sufficient = False
            if is_adjust_condition_sufficient:
                bid = bid_operator.adjust(media, **status)
                result['contents'].append(bid)
                print('[facebook_adapter.main]: adset_id: {}, bid is {}'.format(adset_id, bid['bid']))
                adset = collector.AdSets(adset_id, database_fb=database_fb)
                adset.get_adset_features()
                bid['pred_cpc'] = bid.pop('bid')
                bid['pred_cpc'] = int( bid['pred_cpc'] )
                bid["status"] = adset.status
                try:
                    next_bidding = bid['pred_cpc']
                    ai_logger.save_adset_behavior(adset_id, ai_logger.BehaviorType.ADJUST, next_bidding)
                    adset.update(next_bidding)
                except Exception as e:
                    print('[main]: update bid unavailable..', e)
                    pass
            del s
        del fb
    else:
        campaign_running_list = database_fb.get_running_campaign().to_dict('records')
        print('[campaign id list]: {}'.format([campaign['campaign_id'] for campaign in campaign_running_list]))
        for campaign in campaign_running_list:
            campaign_id = campaign.get("campaign_id")
            print('==========[campaign_id]: {} =========='.format(campaign_id))
            collector_campaign = collector.Campaigns(campaign_id, database_fb=database_fb)
            result={ 'media': 'Facebook', 'campaign_id': campaign_id, 'contents':[] }
            fb = FacebookCampaignAdapter( campaign_id, database_fb )
            fb.get_df()
            fb.retrieve_campaign_attribute()
            adset_list = collector_campaign.get_adsets_active()
            charge_type = campaign.get("destination_type")
            for adset_id in adset_list:
                s = FacebookAdSetAdapter( adset_id, fb )
                status = s.retrieve_adset_attribute()
                media = result['media']
                is_adjust_condition_sufficient=True
                for key, val in status.items():
                    if val is None:
                        print('[facebook_adapter.main]: insufficient conditions to adjust bid, adset_id ', adset_id)
                        is_adjust_condition_sufficient = False
                if is_adjust_condition_sufficient:
                    bid = bid_operator.adjust(media, **status)
                    result['contents'].append(bid)
                    print('[facebook_adapter.main]: adset_id: {}, bid is {}'.format(adset_id, bid['bid']))
                    adset = collector.AdSets(adset_id, database_fb=database_fb)
                    adset.get_adset_features()
                    bid['pred_cpc'] = bid.pop('bid')
                    bid['pred_cpc'] = int( bid['pred_cpc'] )
                    bid["status"] = adset.status
                    try:
                        next_bidding = bid['pred_cpc']
                        ai_logger.save_adset_behavior(adset_id, ai_logger.BehaviorType.ADJUST, next_bidding)
                        adset.update(next_bidding)
                    except Exception as e:
                        print('[main]: update bid unavailable..', e)
                        pass
                del s
            fb.database_fb.dispose()
            del fb
    database_fb.dispose()
    del database_fb
    print(datetime.datetime.now()-start_time)
    return


# In[ ]:


if __name__=='__main__':
    main()
    import gc
    gc.collect()
#     main(23842953829930431)


# In[ ]:


# !jupyter nbconvert --to script facebook_adapter.ipynb


# In[ ]:




