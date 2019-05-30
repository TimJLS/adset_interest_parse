#!/usr/bin/env python
# coding: utf-8

# In[19]:


import datetime
import pandas as pd
import numpy as np
import gdn_db
import gdn_datacollector
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
        self.mydb = gdn_db.connectDB( DATADASE )
        self.limit = 9000
        self.hour_per_day = 24
        self.campaign_id = campaign_id
        self.request_time = datetime.datetime.now()
        self.time_progress = ( self.request_time.hour + 1 ) / self.hour_per_day
        self.init_bid_dict = dict()
        self.last_bid_dict = dict()
        
    def _get_df(self):
        campaign_sql = "SELECT * FROM campaign_target WHERE campaign_id={}".format( self.campaign_id )
        adgroup_sql = "select * from (select * from adgroup_insights WHERE campaign_id = {} order by request_time desc) as a group by adgroup_id".format( self.campaign_id )
        self.df_camp = pd.read_sql( campaign_sql, con=self.mydb )
        self.df_adgroup = pd.read_sql( adgroup_sql, con=self.mydb )
        return
    
    def _get_bid(self):
        df_init_bid = pd.read_sql( "SELECT * FROM adgroup_initial_bid WHERE campaign_id={} ;".format( self.campaign_id ), con=self.mydb )
        self.get_adgroup_list()
        bid_amount_type = BIDDING_INDEX[ self.df_adgroup['bidding_type'].iloc[0] ]
        for adgroup in self.adgroup_list:
            init_bid = df_init_bid[BID_AMOUNT][df_init_bid.adgroup_id==adgroup].head(1).iloc[0].astype(dtype=object)
#             init_bid = bid_operator.revert_bid_amount(init_bid)
            last_bid = self.df_adgroup[ bid_amount_type ][self.df_adgroup.adgroup_id==adgroup].tail(1).iloc[0].astype(dtype=object)
#             init_bid = bid_operator.revert_bid_amount(init_bid)
#             last_bid = bid_operator.reverse_bid_amount(last_bid)
            self.init_bid_dict.update({ adgroup: init_bid })
            self.last_bid_dict.update({ adgroup: last_bid })
        return
    
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
        self.campaign_performance = self.df_camp[ TARGET ].sum()
        return self.campaign_performance
    
    def get_campaign_target(self):
        self.campaign_target = self.df_camp[ TARGET_LEFT ].iloc[0].astype(dtype=object)
        return self.campaign_target
    
    def get_campaign_day_target(self):
        self.campaign_day_target = self.campaign_target / self.periods_left
        return self.campaign_day_target

    def get_campaign_progress(self):
        self.campaign_progress = self.campaign_performance / self.campaign_day_target
        return self.campaign_progress
    
    def get_adgroup_list(self):
        try:
            self.df_adgroup
        except:
            self._get_df()
        self.adgroup_list = self.df_adgroup[ ADGROUP_ID ][
            ( self.df_adgroup.request_time.dt.date == self.request_time.date() )
        ].unique().tolist()
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
        self.mydb.close()
        return

class AdGroupAdapter(CampaignAdapter):
    def __init__(self, adgroup_id, camp):
        self.mydb = gdn_db.connectDB( DATADASE )
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
        print(self.df_adgroup[ CAMPAIGN_ID ].iloc[0].astype(dtype=object))
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
        except ValueError as e:
            print('[facebook_adapter.AdGroupAdapter.get_adgroup_performance()]', e)
            self.adgroup_performance = 0
        if math.isnan(self.adgroup_performance):
            self.adgroup_performance = 0
        return self.adgroup_performance
    
    def get_bid(self):
        self.init_bid = self.init_bid_dict[self.adgroup_id]
        self.last_bid = self.last_bid_dict[self.adgroup_id]
        return
    
    def get_adgroup_time_target(self):
        self.adgroup_time_target = self.adgroup_day_target * self.time_progress
        return self.adgroup_time_target
    
    def get_adgroup_progress(self):
#         print(self.adgroup_performance, self.adgroup_time_target)
        self.adgroup_progress = self.adgroup_performance / self.adgroup_time_target
        return self.adgroup_progress
    
    def retrieve_adgroup_attribute(self):
        self.init_campaign(self.camp)
        self.get_campaign_id()
        self.get_adgroup_day_target()
        self.get_adgroup_performance()
        self.get_bid()
        self.get_adgroup_time_target()
        self.get_adgroup_progress()
        self.mydb.close()
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
    campaign_id_list = gdn_db.get_campaign()['campaign_id'].unique()
    for campaign_id in campaign_id_list:
        print(campaign_id)
        campaign_id = campaign_id.astype(dtype=object)
        result={ 'media': 'GDN', 'campaign_id': campaign_id, 'contents':[] }
        release_version_result = {  }
#         try:
        camp = CampaignAdapter( campaign_id )
        camp.retrieve_campaign_attribute()
        adgroup_list = camp.get_adgroup_list()
        destination_type = camp.df_camp['destination_type'].iloc[0]
        account_id = camp.df_camp['customer_id'].iloc[0]
        for adgroup in adgroup_list:
            s = AdGroupAdapter( adgroup, camp )
            status_dict = s.retrieve_adgroup_attribute()
            media = result['media']
            bid_dict = bid_operator.adjust(media, **status_dict)
            gdn_datacollector.update_adgroup_bid(account_id, adgroup, bid_dict['bid'])
            result['contents'].append(bid_dict)
            del s
        
        mydict_json = json.dumps(result, cls=MyEncoder)
        release_json = json.dumps(release_version_result)
        gdn_db.insert_result( campaign_id, mydict_json )
        del camp
#         except:
#             print('pass')
#             pass
        
#     campaign_id = 1747836664
#     result={ 'media': 'GDN', 'campaign_id': campaign_id, 'contents':[] }
#     camp = CampaignAdapter( campaign_id )
#     camp.retrieve_campaign_attribute()
#     adgroup_list = camp.get_adgroup_list()
#     for adgroup in adgroup_list:
#         s = AdGroupAdapter( adgroup, camp )
#         status = s.retrieve_adgroup_attribute()
#         media = result['media']
#         bid = bid_operator.adjust(media, **status)
#         result['contents'].append(bid)
#         print(result)
#         del s
#     del camp
    
    print(datetime.datetime.now()-start_time)
    return
    


# In[ ]:


if __name__=='__main__':
    main()
    import gc
    gc.collect()


# In[ ]:


get_ipython().system('jupyter nbci')

