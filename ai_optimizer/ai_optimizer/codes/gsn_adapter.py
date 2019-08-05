#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gsn_db
import gsn_datacollector
import google_adwords_controller as controller
import gdn_gsn_ai_behavior_log as logger
from gdn_gsn_ai_behavior_log import BehaviorType
import bid_operator

import datetime
import pandas as pd
import numpy as np
import json
import math


# In[2]:


DATABASE = "dev_gsn"
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


# In[3]:


class Field:
    ai_start_date = 'ai_start_date'
    ai_stop_date = 'ai_stop_date'
    ad_group_id = 'adgroup_id'
    campaign_id = 'campaign_id'
    keyword_id = 'keyword_id'
    target = 'target'
    bid_amount = 'bid_amount'
    target_left = 'target_left'
    init_bid = 'init_bid'
    last_bid = 'last_bid'
    keyword_progress = 'keyword_progress'
    campaign_progress = 'campaign_progress'
    period = 'period'


# In[4]:


class CampaignAdapter(object):
    def __init__(self, campaign_id):
        self.mydb = gsn_db.connectDB( DATABASE )
        self.hour_per_day = 24
        self.campaign_id = campaign_id
        self.request_time = datetime.datetime.now()
        self.time_progress = ( self.request_time.hour + 1 ) / self.hour_per_day
        self.init_bid_dict = dict()
        self.last_bid_dict = dict()
        
    def _get_df(self):
        campaign_sql = "SELECT * FROM campaign_target WHERE campaign_id={}".format( self.campaign_id )
        keyword_sql = "select * from keywords_insights WHERE campaign_id = {} AND DATE(request_time) = '{}'".format( self.campaign_id, self.request_time.date() )
        self.df_camp = pd.read_sql( campaign_sql, con=self.mydb )
        self.df_keyword = pd.read_sql( keyword_sql, con=self.mydb )
        return
    
    def get_keyword_id_list(self):
        keyword_id_list_sql = "select DISTINCT keyword_id from (select * from keywords_insights WHERE campaign_id = {} and status='enabled' order by request_time) as a group by keyword_id".format( self.campaign_id )
        self.mycursor = self.mydb.cursor()
        self.mycursor.execute( keyword_id_list_sql )
        default = self.mycursor.fetchall()
        self.keyword_id_list = [ i[0] for i in default ]
        return self.keyword_id_list
    
    def _get_bid(self):
        df_init_bid = pd.read_sql( "SELECT * FROM adgroup_initial_bid WHERE campaign_id={} ;".format( self.campaign_id ), con=self.mydb )
        self.get_keyword_id_list()
        bid_amount_type = BIDDING_INDEX[ self.df_keyword['bidding_type'].iloc[0] ]
        for keyword_id in self.keyword_id_list:
            if len(self.df_keyword[self.df_keyword.keyword_id==keyword_id]) != 0:
                init_bid = df_init_bid[Field.bid_amount][df_init_bid.keyword_id==keyword_id].head(1).iloc[0].astype(dtype=object)
                last_bid = self.df_keyword[ bid_amount_type ][self.df_keyword.keyword_id==keyword_id].tail(1).iloc[0].astype(dtype=object)
                self.init_bid_dict.update({ keyword_id: init_bid })
                self.last_bid_dict.update({ keyword_id: last_bid })
        return
        
    def get_periods_left(self):
        self.periods_left = ( self.df_camp[ Field.ai_stop_date ].iloc[0] - self.request_time.date() ).days + 1
        return self.periods_left
    
    def get_periods(self):
        self.periods = ( self.df_camp[ Field.ai_stop_date ].iloc[0] - self.df_camp[ Field.ai_start_date ].iloc[0] ).days
        return self.periods
    
    def get_campaign_performance(self):
        self.campaign_performance = self.df_camp[ Field.target ].div(self.df_camp[ Field.period ] ).sum()
        return self.campaign_performance
    
    def get_campaign_target(self):
        self.campaign_target = self.df_camp[ Field.target_left ].iloc[0].astype(dtype=object)
        return self.campaign_target
    
    def get_campaign_day_target(self):
        self.campaign_day_target = self.campaign_target / self.periods_left
        return self.campaign_day_target

    def get_campaign_progress(self):
        self.campaign_progress = self.campaign_performance / self.campaign_day_target
        self.campaign_progress = 1 if self.campaign_day_target <= 0 else self.campaign_progress
        return self.campaign_progress
        
    def retrieve_campaign_attribute(self):
        self._get_df()
        self.get_keyword_id_list()
        self._get_bid()
        self.get_periods_left()
        self.get_periods()
        self.get_campaign_performance()
        self.get_campaign_target()
        self.get_campaign_day_target()
        self.get_campaign_progress()
        self.mydb.close()
        return


# In[5]:


class KeywordGroupAdapter(object):
    def __init__(self, keyword_id, ad_group_id, camp):
        self.mydb = gsn_db.connectDB( DATABASE )
        self.keyword_id = keyword_id
        self.ad_group_id = ad_group_id
        self.camp = camp
    
    def get_campaign_id(self):
        self.campaign_id = self.camp.df_keyword[ Field.campaign_id ].iloc[0].astype(dtype=object)
        return self.campaign_id
    
    def get_keyword_day_target(self):
        keyword_num = len( self.camp.keyword_id_list )
        self.keyword_day_target = self.camp.campaign_day_target / keyword_num
        return self.keyword_day_target
    
    def get_keyword_performance(self):
        try:
            target_performance_index = DESTINATION_INDEX[self.camp.df_camp['destination_type'].iloc[0]]
            self.keyword_performance = self.camp.df_keyword[(self.camp.df_keyword.keyword_id==self.keyword_id)&(self.camp.df_keyword.adgroup_id==self.ad_group_id)][[ target_performance_index ]].tail(1).iloc[0,0]
        except Exception as e:
            print('[gsn_adapter.KeywordGroupAdapter.get_keyword_performance()]', e)
            self.keyword_performance = 0
        if math.isnan(self.keyword_performance):
            self.keyword_performance = 0
        return self.keyword_performance
    
    def get_bid(self):
        self.init_bid = self.camp.init_bid_dict[self.keyword_id]
        self.last_bid = self.camp.last_bid_dict[self.keyword_id]
        return
    
    def get_keyword_time_target(self):
        self.keyword_time_target = self.keyword_day_target * self.camp.time_progress
        return self.keyword_time_target
    
    def get_keyword_progress(self):
        self.keyword_progress = self.keyword_performance / self.keyword_time_target
        self.keyword_progress = 1 if self.keyword_time_target <= 0 else self.keyword_progress
        return self.keyword_progress
    
    def retrieve_keyword_attribute(self):
        self.get_campaign_id()
        self.get_keyword_day_target()
        self.get_keyword_performance()
        self.get_bid()
        self.get_keyword_time_target()
        self.get_keyword_progress()
        self.mydb.close()
        return {
            Field.keyword_id:self.keyword_id,
            Field.init_bid:self.init_bid,
            Field.last_bid:self.last_bid,
            Field.keyword_progress:self.keyword_progress,
            Field.campaign_progress:self.camp.campaign_progress
        }


# In[6]:



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
    campaign_dict_list = gsn_db.get_campaign().to_dict('records')
    for campaign_dict in campaign_dict_list:
        campaign_id = campaign_dict['campaign_id']
        destination_type = campaign_dict['destination_type']
        account_id = campaign_dict['customer_id']
        

        result={ 'media': 'GSN', 'campaign_id': campaign_id, 'contents':[] }
        print('[campaign_id]: ', campaign_id)
        service_container = controller.AdGroupServiceContainer(account_id)
        adapter_campaign = CampaignAdapter( campaign_id )
        controller_campaign = controller.Campaign(service_container, campaign_id=campaign_id)
        controller_campaign.get_ad_groups()
        adapter_campaign.retrieve_campaign_attribute()
        adgroup_list = controller_campaign.ad_groups
        
        
        controller_campaign.get_keywords()
        for controller_keyword in controller_campaign.keywords:
            print('[keyword_id]: ', controller_keyword.keyword_id)

            adapter_keyword = KeywordGroupAdapter( 
                controller_keyword.keyword_id, controller_keyword.ad_group.ad_group_id, adapter_campaign )
            status_dict = adapter_keyword.retrieve_keyword_attribute()
            media = result['media']
            bid_dict = bid_operator.adjust(media, **status_dict)

            ad_group_pair = {
                'db_type': 'dev_gsn', 'campaign_id': campaign_id, 'adgroup_id': controller_keyword.ad_group.ad_group_id,
                'criterion_id': controller_keyword.keyword_id, 'criterion_type': 'keyword'
            }
            logger.save_adgroup_behavior(
                behavior_type=BehaviorType.ADJUST, behavior_misc=bid_dict['bid'], **ad_group_pair)

            controller_keyword.update_bid(bid_micro_amount=bid_dict['bid'])                   
            result['contents'].append(bid_dict)
            del adapter_keyword
        
        mydict_json = json.dumps(result, cls=MyEncoder)
        gsn_db.insert_result( campaign_id, mydict_json )
        del adapter_campaign
        del controller_campaign
    
    print(datetime.datetime.now()-start_time)
    return


# In[7]:


if __name__=='__main__':
    main()


# In[8]:


#!jupyter nbconvert --to script gsn_adapter.ipynb


# In[ ]:




