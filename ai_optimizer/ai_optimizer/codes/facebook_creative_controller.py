#!/usr/bin/env python
# coding: utf-8

# In[1]:


import facebook_business.adobjects.campaign as facebook_business_campaign
import facebook_business.adobjects.adset as facebook_business_adset
import facebook_business.adobjects.ad as facebook_business_ad
import sys
import adgeek_permission as permission
import database_controller as db_controller
import facebook_datacollector as data_collector
from datetime import date,timedelta

TABLE_NAME_CREATIVE_LOG = 'adset_creative_log'
IMPRESSION_THRESHOLD = 500
CHECK_DIFF_DAY = 7
IS_DEBUG = False


# In[2]:


class Creative(object):
    def __init__( self, ad_id, collector_campaign ):
        self.ad_id = ad_id
        self.campaign = collector_campaign
        self.insights = dict.fromkeys(data_collector.GENERAL_FIELD.values(), 0)
        self.impressions = self.insights.get("impressions", 0)
        self.reach = self.insights.get("reach", 0)
        self.spend = self.insights.get("spend", 0)
        self.__get_insights()
#         self.cost_per_reach = round(self.spend/reach ,5)
    def __str__(self):
        return 'ad_id:' + str(self.ad_id) + ' reach:' + str(self.reach) + ' spend:' + str(self.spend)+ ' cost_per_reach:' + str(self.cost_per_reach)
    def update_as_close(self):
        print('[Creative] ad_id close:', self.ad_id)
        if not IS_DEBUG:
            this_ad = facebook_business_ad.Ad(self.ad_id)
            this_ad['status'] = 'PAUSED'
            this_ad.remote_update()
    def __get_insights(self):
        this_ad = facebook_business_ad.Ad(self.ad_id)
        params = {
            'time_range[since]': self.campaign.ai_start_date,
            'time_range[until]': self.campaign.ai_stop_date,
        }
        insights = this_ad.get_insights( 
            params = params,
            fields = list( data_collector.GENERAL_FIELD.values() )+list( data_collector.TARGET_FIELD.values() ) )
        if len(insights) > 0:
            current_creative = insights[0]
            if current_creative.get( data_collector.TARGET_FIELD[data_collector.Field.thruplay_actions] ) and self.campaign.destination_type=='THRUPLAY':
                actions_list = current_creative.get( data_collector.TARGET_FIELD[data_collector.Field.thruplay_actions] )
                for act in actions_list:
                    if act["action_type"] == data_collector.CAMPAIGN_OBJECTIVE_FIELD[ self.campaign.destination_type ]:
                        target = int( act.get("value") ) if act.get("value") else 0
                        self.insights.update( {"action": target} )
                
            elif current_creative.get( data_collector.Field.actions ) and data_collector.FUNNEL_METRICS.get( self.campaign.destination_type ):
                actions_list = current_creative.get( data_collector.Field.actions )
                actions_dict = data_collector.FUNNEL_METRICS.get( self.campaign.destination_type )
                if self.campaign.custom_conversion_id:
                    custom_conversion_key = 'offsite_conversion.custom.' + str(self.campaign.custom_conversion_id)
                    self.insights.update( { "action": 0 } )
                    action_value = [int(act["value"]) for act in actions_list if act["action_type"] == custom_conversion_key]
                    self.insights.update( dict( zip( ["action"], action_value ) ) )
                else:
                    action_value = [int(act["value"]) for act in actions_list if act["action_type"] == data_collector.CAMPAIGN_OBJECTIVE_FIELD[self.campaign.destination_type]]
                    self.insights.update(
                        dict( zip( ["action"], action_value ) )
                    )
            for campaign_field in list( data_collector.GENERAL_FIELD.keys() ):
                self.insights.update( {campaign_field: float(current_creative.get(campaign_field, 0))} )
            # Deal with those metrics not in 'actions' metric
            if self.campaign.destination_type == 'ALL_CLICKS':
                '''assign to field target and cost_per_target'''
                self.insights[ "action" ] = int(self.insights.pop( data_collector.Field.clicks ))
                self.insights.pop( data_collector.Field.cpc )
            elif self.campaign.destination_type == 'REACH':
                self.insights[ "action" ] = int(self.insights[ data_collector.Field.reach ])
                self.insights.pop( data_collector.Field.clicks )
                self.insights.pop( data_collector.Field.cpc )
            elif self.campaign.destination_type == 'IMPRESSIONS':
                self.insights[ "action" ] = int(self.insights[ data_collector.Field.impressions ])
                self.insights.pop( data_collector.Field.clicks )
                self.insights.pop( data_collector.Field.cpc )
            else:
                self.insights.pop( data_collector.Field.clicks )
                self.insights.pop( data_collector.Field.cpc )
        self.impressions, self.reach, self.spend, self.action = self.insights['impressions'], self.insights['reach'], self.insights['spend'], self.insights.get('action', 0)
        self.cost_per_reach = (self.spend / self.reach) if self.reach != 0 else sys.maxsize
        self.cost_per_action = (self.spend / self.action) if self.action != 0 else sys.maxsize
        self.insights.update({
            "cost_per_reach": self.cost_per_reach,
            "cost_per_action": self.cost_per_action
        })
        return self.insights
        
def get_active_adsets_by_campaign(campaign_id):
    active_adsets_list = list()
    this_campaign = facebook_business_campaign.Campaign(campaign_id)
    adsets = this_campaign.get_ad_sets( fields = ['id','status', 'name'])
    for adset_id in adsets:
        if  adset_id.get("status") == 'ACTIVE' :
            active_adsets_list.append( int(adset_id.get("id")) )
    return active_adsets_list    

def get_active_ads_by_adset(adset_id):
    active_ad_list = list()
    this_adset = facebook_business_adset.AdSet(adset_id)
    this_ads = this_adset.get_ads( fields = ['id','status', 'name'])
    for ad in this_ads:
        if  ad.get("status") == 'ACTIVE' :
            active_ad_list.append( ad.get("id") )
            
    return active_ad_list    

#list of alive creative instances 
def get_creative_structure(collector_campaign, active_ad_list):
    
    creative_list = [ Creative(ad_id, collector_campaign) for ad_id in active_ad_list ]
    creative_list = [ creative for creative in creative_list if creative.impressions > 0 ]
    
    return creative_list


# In[3]:


def process_creative_for_adset(collector_campaign, adset_id):

    active_ad_list = get_active_ads_by_adset(adset_id)
    print('[process_creative_for_adset] adset_id:', adset_id, ' active_ad_list:', active_ad_list)
    
    if len(active_ad_list) <= 1:
        print('[process_creative_for_adset] active_ad_list too few')
        return
    
    creative_list = get_creative_structure(collector_campaign, active_ad_list)
    
    if len(active_ad_list) != len(creative_list):
        print('[process_creative_for_adset] some ad does not have insight , need every ad has insight')
        return     
        
    is_all_adset_reach_enough = True
    creative_lowest_cpr = 0
    hightest_cost_creative = None
    
    for c in creative_list:
        print(c)
        if  c.reach < IMPRESSION_THRESHOLD:
            is_all_adset_reach_enough = False
        if c.cost_per_action > creative_lowest_cpr:
            creative_lowest_cpr = c.cost_per_action
            hightest_cost_creative = c
        
    if not is_all_adset_reach_enough:
        print('[process_creative_for_adset] some ad reach not enough')
        return
    else:
        #process this adset's creative , need to log to db
        database_fb = db_controller.FB(db_controller.Database())
        database_fb.upsert(TABLE_NAME_CREATIVE_LOG, {'campaign_id':collector_campaign.campaign_id, 'adset_id':adset_id, 'process_date': date.today() })

        #need to decide which to close
        if hightest_cost_creative:
            print('[process_creative_for_adset] lowest cost_per_action, ad:', hightest_cost_creative.ad_id, 'cost_per_action:', hightest_cost_creative.cost_per_action)
            hightest_cost_creative.update_as_close()

        print('[process_creative_for_adset] adset_id process finish', adset_id)
    
def is_need_to_process(campaign_id, adset_id):
    database_fb = db_controller.FB(db_controller.Database())
    df = database_fb.retrieve_all(TABLE_NAME_CREATIVE_LOG)
    adset_id_list = list(df['adset_id'])
    
    if adset_id not in adset_id_list:
        #first time check , insert new adset 
        print('[is_need_to_process] upsert adset_id:', adset_id)
        database_fb.upsert(TABLE_NAME_CREATIVE_LOG, {'campaign_id':campaign_id, 'adset_id':adset_id, 'process_date': date.today()})
        return False
    else:
        df_date = df[df['adset_id'] == adset_id ]
        last_process_date = df_date.process_date.iloc[0]
        diff_days = ( date.today() - last_process_date).days
        print('[is_need_to_process] check diff day:', diff_days, ' adset_id:', adset_id)
        return diff_days >= CHECK_DIFF_DAY

def process_active_adsets(collector_campaign, active_adsets_list):
    print('[process_active_adsets] active_adsets_list:', active_adsets_list)
    
    for adset_id in active_adsets_list:
        adset_id = int(adset_id)
        if is_need_to_process(collector_campaign.campaign_id, adset_id):
            process_creative_for_adset(collector_campaign, adset_id)
        else:
            print('[process_active_adsets] no need to process' , adset_id)
        print('==')


# In[4]:


if __name__ == '__main__':
    database_fb = db_controller.FB(db_controller.Database())
    performance_campaign = database_fb.get_performance_campaign()

    for index, row in performance_campaign.iterrows():
        account_id = row['account_id']
        campaign_id = row['campaign_id']
        is_creative_opt = eval(row['is_creative_opt'])
        if is_creative_opt:
            permission.init_facebook_api(account_id)
            collector_campaign = data_collector.Campaigns(campaign_id, database_fb)
            active_adsets_list = collector_campaign.get_adsets_active()
            print('campaign:', campaign_id, ' active_adsets_list:', active_adsets_list)
            process_active_adsets(collector_campaign, active_adsets_list)
        print('===========next campaign===========')
    print('===========finish all campaign===========')
 


# In[5]:


# !jupyter nbconvert --to script facebook_creative_controller.ipynb


# In[ ]:




