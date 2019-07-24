#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gsn_datacollector as datacollector
import gsn_db
from googleads import adwords

import datetime
import pandas as pd
import copy
import math
IS_DEBUG = False
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
DATABASE = "dev_gsn"
DATE = datetime.datetime.now().date()
DATETIME = datetime.datetime.now()
AGE_RANGE_LIST = [503001,503002,503003,503004,503005,503006,503999,]


# In[2]:


def modify_opt_result_db(campaign_id, is_optimized):
    #get date
    opt_date = datetime.datetime.now()
    #insert to table date and Ture for is_opt
    sql = "update campaign_target set is_optimized = '{}', optimized_date = '{}' where campaign_id = {}".format(is_optimized, opt_date, campaign_id)
    mydb = gsn_db.connectDB(DATABASE)
    mycursor = mydb.cursor()
    try:
        mycursor.execute(sql)
    except Exception as e:
        print('[gsn_externals] modify_opt_result_db: ', e)
    finally:
        mydb.commit()
        mydb.close()


# In[3]:


def optimize_performance_campaign():
    return


# In[4]:


def optimize_branding_campaign():
    df_branding_campaign = gsn_db.get_branding_campaign_is_running()
    campaign_id_list = df_branding_campaign['campaign_id'].tolist()
    print('[optimize_branding_campaign]: campaign_id_list', campaign_id_list)
    for campaign_id in campaign_id_list:
        print('[optimize_branding_campaign] campaign_id', campaign_id)
        customer_id = df_branding_campaign['customer_id'][df_branding_campaign.campaign_id==campaign_id].iloc[0]
        destination_type = df_branding_campaign['destination_type'][df_branding_campaign.campaign_id==campaign_id].iloc[0]
        daily_target = df_branding_campaign['daily_target'][df_branding_campaign.campaign_id==campaign_id].iloc[0]

        destination = df_branding_campaign['destination'][df_branding_campaign.campaign_id==campaign_id].iloc[0]
        ai_spend_cap = df_branding_campaign['ai_spend_cap'][df_branding_campaign.campaign_id==campaign_id].iloc[0]
        original_cpa = ai_spend_cap/destination
        print('[optimize_branding_campaign]  original_cpa:' , original_cpa)

        adwords_client.SetClientCustomerId( customer_id )
        
        objective = 'clicks'
        # Init datacollector Campaign
        camp = datacollector.Campaign(customer_id, campaign_id)
        campaign_day_insights_dict = camp.get_campaign_insights(adwords_client, date_preset=datacollector.DatePreset.yesterday)
        print('[optimize_branding_campaign] campaign_day_insights_dict', campaign_day_insights_dict)
        campaign_lifetime_insights_dict = camp.get_campaign_insights(adwords_client, date_preset=datacollector.DatePreset.lifetime)
        print('[optimize_branding_campaign] campaign_lifetime_insights_dict', campaign_lifetime_insights_dict)
        
        campaign_day_insights_dict['original_cpa'] = original_cpa
        campaign_day_insights_dict['daily_target'] = daily_target
        campaign_day_insights_dict['achieving_rate'] = int( campaign_day_insights_dict[objective] ) / daily_target
        campaign_day_insights_dict['target'] = campaign_day_insights_dict[objective]
        print('[optimize_branding_campaign][achieving rate]', campaign_day_insights_dict['achieving_rate'], '[target]', campaign_day_insights_dict[objective], '[daily_target]', daily_target)
        optimize_list = ['customer_id', 'campaign_id', 'daily_target', 'daily_budget', 'spend', 'cost_per_target', 'target', 'original_cpa', 'achieving_rate']
        campaign_status_dict = dict([(key, value) for key, value in campaign_day_insights_dict.items() if key in optimize_list])
#         print(campaign_status_dict)
        handle_initial_bids(**campaign_status_dict)


# In[36]:


def handle_initial_bids(customer_id, campaign_id, spend, daily_budget, cost_per_target, target, original_cpa, daily_target, achieving_rate):
    if IS_DEBUG:
        print('[handle_initial_bids] IS_DEBUG == True , not adjust bid')
        return

    if daily_target < 0:
        if gsn_db.get_current_init_bid(campaign_id=campaign_id) >= original_cpa:
            print('[handle_initial_bids] lifetime target has reached, no optimize.')
            gsn_db.update_init_bid(campaign_id=campaign_id, bid_ratio=0.9)
            modify_opt_result_db(campaign_id , True)
        else:
            print('[handle_initial_bids] good enough , keep the bid', ', original_cpa:', original_cpa)
            modify_opt_result_db(campaign_id , False)
        return
    
    if 0 <= achieving_rate < 1:
        if spend >= 1.5 * daily_budget:
            print('[handle_initial_bids] achieving_rate is not OK, while spending is.')
            print('[handle_initial_bids] We might have KPI setting issues, contact AM/OPT')
            print('[handle_initial_bids] stay_init_bid')
        elif spend < 0.8 * daily_budget:
            print('[handle_initial_bids] achieving_rate is not OK, spending is not.')
            print('[handle_initial_bids] adjust init_bid up the bid')
            gsn_db.update_init_bid(campaign_id=campaign_id, bid_ratio=1.1)
        else:
            print('[handle_initial_bids] achieving_rate is not OK, spending is just fine.')
        return
    else:     
        if spend < 0.8 * daily_budget:
            print('[handle_initial_bids] achieving_rate is OK, but spending is not.')
            print('[handle_initial_bids] increase init_bid of keywords which is low ranking and under KPI.')
            
            camp = datacollector.Campaign(customer_id, campaign_id)
            keyword_insights_dict_list = camp.get_keyword_insights(date_preset=datacollector.DatePreset.lifetime)
            for keyword_insights_dict in keyword_insights_dict_list:
                if keyword_insights_dict['position'] >= 2 or keyword_insights_dict['position'] <1 and keyword_insights_dict['cost_per_target'] < original_cpa:
                    keyword_id = keyword_insights_dict['keyword_id']
                    ad_group_id = keyword_insights_dict['adgroup_id']
                    gsn_db.update_init_bid(keyword_id=keyword_id, ad_group_id=ad_group_id, bid_ratio=1.1)
                    modify_opt_result_db(campaign_id , True)
        else:
            print('[handle_initial_bids] everything is fine.')
            modify_opt_result_db(campaign_id , False)
        return


# In[ ]:


if __name__=="__main__":
    start_time = datetime.datetime.now()
    print('current time: ', start_time)
    optimize_performance_campaign()
    optimize_branding_campaign()
    print(datetime.datetime.now() - start_time)


# In[ ]:


#!jupyter nbconvert --to script gsn_externals.ipynb

