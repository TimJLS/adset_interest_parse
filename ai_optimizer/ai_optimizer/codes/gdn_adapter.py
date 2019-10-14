#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import datetime
import pandas as pd
import numpy as np
import gdn_datacollector
import google_adwords_controller as controller
import database_controller
import gdn_gsn_ai_behavior_log as logger
from gdn_gsn_ai_behavior_log import BehaviorType
import bid_operator
import json
import math

DATADASE = "dev_gdn"
START_TIME = 'start_time'
STOP_TIME = 'stop_time'
AI_START_DATE = 'ai_start_date'
AI_STOP_DATE = 'ai_stop_date'
AD_ID = 'ad_id'
ADGROUP_ID = 'adgroup_id'
CAMPAIGN_ID = 'campaign_id'
CHARGE = 'charge'
TARGET = 'target'
BID_AMOUNT = 'bid_amount'
REQUEST_TIME = 'request_time'
TARGET_LEFT = 'target_left'

INIT_BID = 'init_bid'
LAST_BID = 'last_bid'
ADGROUP_PROGRESS = 'adgroup_progress'
CAMPAIGN_PROGRESS = 'campaign_progress'
BIDDING_INDEX = {
    'cpc': 'cpc_bid',
    'Target CPA': 'cpa_bid',
}
DESTINATION_INDEX = {
    'cpc': 'clicks',
    'cpa': 'conversions',
    'LINK_CLICKS': 'clicks',
    'CONVERSIONS':'conversions',
}
class CampaignAdapter(object):
    def __init__(self, campaign_id):
        self.limit = 9000
        self.hour_per_day = 24
        self.campaign_id = campaign_id
        self.request_time = datetime.datetime.now()
        self.time_progress = ( self.request_time.hour + 1 ) / self.hour_per_day
        self.init_bid_dict = dict()
        self.last_bid_dict = dict()
        
    def _get_df(self):
        self.df_camp = database_gdn.retrieve("campaign_target", campaign_id=self.campaign_id, by_request_time=False)
        # here cost 5 seconds
        self.df_adgroup = database_gdn.retrieve("table_insights", campaign_id=self.campaign_id)
        return
    
    def _get_bid(self):
        self.get_adgroup_list()
        for adgroup in self.adgroup_list:
            try:
                init_bid = database_gdn.get_init_bid(adgroup)
                self.init_bid_dict.update({ adgroup: init_bid })
                # last bid costs time the most
                last_bid = database_gdn.get_last_bid(adgroup)
                self.last_bid_dict.update({ adgroup: last_bid })
            except Exception as e:
                print('[facebook_adapter.get_bid]: lack init_bid or last_bid. ', e)
                pass
    
    def get_periods_left(self):
        self.periods_left = 0
        try:
            self.periods_left = ( self.df_camp[ AI_STOP_DATE ].iloc[0] - self.request_time.date() ).days + 1
        except:
            self.periods_left = ( datetime.datetime.now().date() - self.request_time.date() ).days + 1
        finally:
            return self.periods_left
    
    def get_periods(self):
        try:
            self.periods = ( self.df_camp[ AI_STOP_DATE ].iloc[0] - self.df_camp[ AI_START_DATE ].iloc[0] ).days
        except:
            self.periods = ( datetime.datetime.now() - self.df_camp[ AI_START_DATE ].iloc[0] ).days
        return self.periods
    
    def get_campaign_performance(self):
        self.campaign_performance = self.df_camp[ TARGET ].div(self.df_camp[ 'period' ] ).sum()
        return self.campaign_performance
    
    def get_campaign_target(self):
        self.campaign_target = self.df_camp[ TARGET_LEFT ].iloc[0].astype(dtype=object)
        return self.campaign_target
    
    def get_campaign_day_target(self):
        self.campaign_day_target = self.campaign_target / self.periods_left
        return self.campaign_day_target

    def get_campaign_progress(self):
        self.campaign_progress = self.campaign_performance / self.campaign_day_target
        self.campaign_progress = 1 if self.campaign_day_target <= 0 else self.campaign_progress
        return self.campaign_progress
    
    def get_adgroup_list(self):
        self.df_ad = database_gdn.retrieve('table_insights', self.campaign_id).to_dict('records')
        self.adgroup_list = [adgroup['adgroup_id'] for adgroup in self.df_ad if adgroup['status']=='enabled']
        return self.adgroup_list
    
    def retrieve_campaign_attribute(self):
        self._get_df()
        self.get_adgroup_list()
        self._get_bid()
        self.get_periods_left()
        self.get_periods()
        self.get_campaign_performance()
        self.get_campaign_target()
        self.get_campaign_day_target()
        self.get_campaign_progress()
        database_gdn.dispose()
        return

class AdGroupAdapter(CampaignAdapter):
    def __init__(self, adgroup_id, camp):
        self.adgroup_id = adgroup_id
        self.camp = camp

    def init_campaign(self, camp):
        self.time_progress = camp.time_progress
        self.limit = camp.limit
        self.hour_per_day = camp.hour_per_day
        self.request_time = camp.request_time
        self.df_adgroup = camp.df_adgroup
        self.df_camp = camp.df_camp
        self.init_bid_dict = camp.init_bid_dict
        self.last_bid_dict = camp.last_bid_dict
        self.periods_left = camp.periods_left
        self.periods = camp.periods
        self.campaign_performance = camp.campaign_performance
        self.campaign_target = camp.campaign_target
        self.campaign_day_target = camp.campaign_day_target
        self.campaign_progress = camp.campaign_progress
        return
    
    def get_campaign_id(self):
        self.campaign_id = self.df_adgroup[ CAMPAIGN_ID ].iloc[0].astype(dtype=object)
        return self.campaign_id
    
    def get_adgroup_day_target(self):
        adgroup_num = len( self.camp.adgroup_list )
        self.adgroup_day_target = self.camp.campaign_day_target / adgroup_num
        return self.adgroup_day_target
    
    def get_adgroup_performance(self):
        try:
            target_performance_index = DESTINATION_INDEX[self.df_camp['destination_type'].iloc[0]]
            self.adgroup_performance = self.df_adgroup[self.df_adgroup.adgroup_id==self.adgroup_id][[ target_performance_index ]].tail(1).iloc[0,0]
        except Exception as e:
            print('[facebook_adapter.AdGroupAdapter.get_adgroup_performance()]', e)
            self.adgroup_performance = 0
        if math.isnan(self.adgroup_performance):
            self.adgroup_performance = 0
        return self.adgroup_performance
    
    def get_bid(self):
        self.init_bid = self.init_bid_dict.get(self.adgroup_id) 
        self.last_bid = self.last_bid_dict.get(self.adgroup_id)
        return
    
    def get_adgroup_time_target(self):
        self.adgroup_time_target = self.adgroup_day_target * self.time_progress
        return self.adgroup_time_target
    
    def get_adgroup_progress(self):
#         print(self.adgroup_performance, self.adgroup_time_target)
        self.adgroup_progress = self.adgroup_performance / self.adgroup_time_target
        self.adgroup_progress = 1 if self.adgroup_time_target <= 0 else self.adgroup_progress
        return self.adgroup_progress
    
    def retrieve_adgroup_attribute(self):
        self.init_campaign(self.camp)
        self.get_campaign_id()
        self.get_adgroup_day_target()
        self.get_adgroup_performance()
        self.get_bid()
        self.get_adgroup_time_target()
        self.get_adgroup_progress()
        return {
            ADGROUP_ID:self.adgroup_id,
            INIT_BID:self.init_bid,
            LAST_BID:self.last_bid,
            ADGROUP_PROGRESS:self.adgroup_progress,
            CAMPAIGN_PROGRESS:self.campaign_progress
        }

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)
        
def main():
    start_time = datetime.datetime.now()
    global database_gdn
    db = database_controller.Database()
    database_gdn = database_controller.GDN(db)
    campaign_running_list = database_gdn.get_running_campaign().to_dict('records')
    for campaign_dict in campaign_running_list:
        campaign_id = campaign_dict['campaign_id']
        destination_type = campaign_dict['destination_type']
        account_id = campaign_dict['customer_id']
        
        print('[campaign_id]: ', campaign_id)
        service_container = controller.AdGroupServiceContainer(account_id)
        adapter_campaign = CampaignAdapter( campaign_id )
        controller_campaign = controller.Campaign(service_container, campaign_id=campaign_id)
        controller_campaign.get_ad_groups()
        # here waste the most time
        adapter_campaign.retrieve_campaign_attribute()
        adgroup_list = controller_campaign.ad_groups
        for controller_ad_group in adgroup_list:
            ad_group_id = controller_ad_group.ad_group_id
            adapter_ad_group = AdGroupAdapter( ad_group_id, adapter_campaign )
            status_dict = adapter_ad_group.retrieve_adgroup_attribute()
            print(status_dict)
            bid_dict = bid_operator.adjust('GDN', **status_dict)
            ad_group_pair = {
                'db_type': 'dev_gdn', 'campaign_id': campaign_id, 'adgroup_id': ad_group_id,
                'criterion_id': ad_group_id, 'criterion_type': 'adgroup'
            }
            logger.save_adgroup_behavior(
                behavior_type=BehaviorType.ADJUST, behavior_misc=bid_dict['bid'], **ad_group_pair)
            controller_ad_group.param.update_bid(bid_micro_amount=bid_dict['bid'])
            del adapter_ad_group
            del controller_ad_group
#         mydict_json = json.dumps(result, cls=MyEncoder)
#         gdn_db.insert_result( campaign_id, mydict_json )
        del adapter_campaign 
    database_gdn.dispose()
    del database_gdn
    print(datetime.datetime.now()-start_time)
    return
    


# In[ ]:


if __name__=='__main__':
    main()
    import gc
    gc.collect()


# In[ ]:


# !jupyter nbconvert --to script gdn_adapter.ipynb


# In[ ]:




