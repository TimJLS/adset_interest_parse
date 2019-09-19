#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gsn_datacollector as datacollector
import google_adwords_controller as controller
import gsn_db
from googleads import adwords

import datetime
import pandas as pd
import copy
import math
IS_DEBUG = False
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
    objective = 'conversions'
    performance_campaign_dict_list = gsn_db.get_performance_campaign_is_running().to_dict('records')
    campaign_id_list = [ performance_campaign_dict['campaign_id'] for performance_campaign_dict in performance_campaign_dict_list ]
    print('[optimize_performance_campaign]: campaign_id_list', campaign_id_list)
    for performance_campaign_dict in performance_campaign_dict_list:
        print('[optimize_performance_campaign] campaign_id', performance_campaign_dict['campaign_id'])
        customer_id = performance_campaign_dict['customer_id']
        campaign_id = branding_campaign_dict['campaign_id']
        destination_type = performance_campaign_dict['destination_type']
        daily_target = performance_campaign_dict['daily_target']

        destination = performance_campaign_dict['destination']
        ai_spend_cap = performance_campaign_dict['ai_spend_cap']
        original_cpa = ai_spend_cap/destination
        print('[optimize_performance_campaign]  original_cpa:' , original_cpa)
        service_container = controller.AdGroupServiceContainer( customer_id )
        
        collector_campaign = collector.Campaign(customer_id, campaign_id)
        campaign_day_insights_dict_list = collector_campaign.get_performance_insights(date_preset=datacollector.DatePreset.yesterday, performance_type='CAMPAIGN').to_dict('records')
        print('[optimize_branding_campaign] campaign_day_insights_dict', campaign_day_insights_dict_list)
#         campaign_lifetime_insights_dict = collector_campaign.get_performance_insights(date_preset=datacollector.DatePreset.lifetime, performance_type='CAMPAIGN').to_dict('records')[0]
#         print('[optimize_branding_campaign] campaign_lifetime_insights_dict', campaign_lifetime_insights_dict)
        if bool(campaign_day_insights_dict_list):
            campaign_day_insights_dict = campaign_day_insights_dict_list[0]
            campaign_day_insights_dict['original_cpa'] = original_cpa
            campaign_day_insights_dict['daily_target'] = daily_target
            campaign_day_insights_dict['achieving_rate'] = int( campaign_day_insights_dict[objective] ) / daily_target
            campaign_day_insights_dict['target'] = campaign_day_insights_dict[objective]
            print('[optimize_branding_campaign][achieving rate]', campaign_day_insights_dict['achieving_rate'], '[target]', campaign_day_insights_dict[objective], '[daily_target]', daily_target)

            handle_campaign_destination(campaign_id, daily_target, original_cpa)
            optimize_list = ['customer_id', 'campaign_id', 'spend', 'daily_budget', 'original_cpa', 'achieving_rate']
            campaign_status_dict = dict([(key, value) for key, value in campaign_day_insights_dict.items() if key in optimize_list])
            handle_initial_bids(**campaign_status_dict)

            if spend >= 1.5 * daily_budget:
                off_keyword_list = get_keyword_off_by_score(campaign_id).to_dict('records')
                adjust_init_bids(off_keyword_list, bid_ratio=0.75)


# In[4]:


def optimize_branding_campaign():
    objective = 'clicks'
    branding_campaign_dict_list = gsn_db.get_branding_campaign_is_running().to_dict('records')
    campaign_id_list = [ branding_campaign_dict['campaign_id'] for branding_campaign_dict in branding_campaign_dict_list ]
    print('[optimize_branding_campaign]: campaign_id_list', campaign_id_list)
    for branding_campaign_dict in branding_campaign_dict_list:
        print('[optimize_branding_campaign] campaign_id', branding_campaign_dict['campaign_id'])
        customer_id = branding_campaign_dict['customer_id']
        campaign_id = branding_campaign_dict['campaign_id']
        destination_type = branding_campaign_dict['destination_type']
        daily_target = branding_campaign_dict['daily_target']

        destination = branding_campaign_dict['destination']
        ai_spend_cap = branding_campaign_dict['ai_spend_cap']
        original_cpa = ai_spend_cap/destination
        print('[optimize_branding_campaign]  original_cpa:' , original_cpa)
        service_container = controller.AdGroupServiceContainer( customer_id )

        # Init datacollector Campaign
        camp = datacollector.Campaign(customer_id, campaign_id)
        campaign_day_insights_dict_list = camp.get_performance_insights(date_preset=datacollector.DatePreset.yesterday, performance_type='CAMPAIGN').to_dict('records')
        
        print('[optimize_branding_campaign] campaign_day_insights_dict', campaign_day_insights_dict_list)
        if bool(campaign_day_insights_dict_list):
            campaign_day_insights_dict = campaign_day_insights_dict_list[0]
    #         campaign_lifetime_insights_dict = camp.get_performance_insights(date_preset=datacollector.DatePreset.lifetime, performance_type='CAMPAIGN').to_dict('records')[0]
    #         print('[optimize_branding_campaign] campaign_lifetime_insights_dict', campaign_lifetime_insights_dict)

            campaign_day_insights_dict['original_cpa'] = original_cpa
            campaign_day_insights_dict['daily_target'] = daily_target
            campaign_day_insights_dict['achieving_rate'] = int( campaign_day_insights_dict[objective] ) / daily_target
            campaign_day_insights_dict['target'] = campaign_day_insights_dict[objective]
            print('[optimize_branding_campaign][achieving rate]', campaign_day_insights_dict['achieving_rate'], '[target]', campaign_day_insights_dict[objective], '[daily_target]', daily_target)

    #         print(campaign_status_dict)
            handle_campaign_destination(campaign_id, daily_target, original_cpa)

            optimize_list = ['customer_id', 'campaign_id', 'spend', 'daily_budget', 'original_cpa', 'achieving_rate']
            campaign_status_dict = dict([(key, value) for key, value in campaign_day_insights_dict.items() if key in optimize_list])
            handle_initial_bids(**campaign_status_dict)


# In[5]:


def get_keyword_off_by_score(campaign_id):
    keywords_for_off = []
    df = gsn_db.get_table(campaign_id, "keywords_score")
    df = df.infer_objects()
    df.request_time = pd.to_datetime(df.request_time)
    df = df[ df.request_time.dt.date == (datetime.datetime.now().date()) ]
    df = df.sort_values(by=['score']).drop_duplicates(["adgroup_id", "keyword_id"]).reset_index(drop=True)
    half = math.ceil( len(df)/2 )
    df_off = df.iloc[:half]
    return df_off


# In[6]:


def handle_campaign_destination(campaign_id, daily_target, original_cpa):
    if IS_DEBUG:
        return
    if daily_target < 0:
        if gsn_db.get_current_init_bid(campaign_id=campaign_id) >= original_cpa:
            print('[handle_initial_bids] lifetime target has reached, no optimize.')
            gsn_db.update_init_bid(campaign_id=campaign_id, bid_ratio=0.9)
            modify_opt_result_db(campaign_id , True)
        else:
            print('[handle_initial_bids] good enough , keep the bid', ', original_cpa:', original_cpa)
            modify_opt_result_db(campaign_id , False)


# In[7]:


def adjust_init_bids(keyword_list, bid_ratio):
    print('[adjust_init_bids] target not enough.')
    for keyword in keyword_list:
        keyword_id = keyword.get('keyword_id')
        ad_group_id = keyword.get('adgroup_id')
        gsn_db.update_init_bid(keyword_id=keyword_id, ad_group_id=ad_group_id, bid_ratio=bid_ratio)


# In[8]:


def handle_initial_bids(customer_id, campaign_id, spend, daily_budget, original_cpa, achieving_rate):
    if IS_DEBUG:
        print('[handle_initial_bids] IS_DEBUG == True , not adjust bid')
        return
    camp = datacollector.Campaign(customer_id, campaign_id)
    keyword_insights_dict_list = camp.get_keyword_insights(date_preset=datacollector.DatePreset.lifetime)
    keyword_insights_dict_list = [
        keyword for keyword in keyword_insights_dict_list if keyword['top_impression_percentage'] <= 0.75
    ]
    if achieving_rate > 1 and spend >= 1.5 * daily_budget:
        print('[handle_initial_bids][spend]: over daily_budget')
        print('[handle_initial_bids][target]: over daily_target')
        adjust_init_bids(keyword_insights_dict_list, 0.75)
        modify_opt_result_db(campaign_id , True)

    elif achieving_rate > 1 and spend < 0.8 * daily_budget:
        print('[handle_initial_bids][spend]: lower daily_budget')
        print('[handle_initial_bids][target]: over daily_target')
        position_keyword_list = [
            keyword for keyword in keyword_insights_dict_list if keyword['cost_per_target'] < original_cpa
        ]
        first_page_keyword_list = [
            keyword for keyword in keyword_insights_dict_list if keyword['first_page_cpc'] >= 2 or keyword['cpc_bid'] <1 and keyword['cost_per_target'] < original_cpa
        ]
        first_page_keyword_list = [
             keyword for keyword in first_page_keyword_list if keyword not in position_keyword_list
        ]
        adjust_init_bids(position_keyword_list, 1.25)
        adjust_init_bids(first_page_keyword_list, 1.25)
        modify_opt_result_db(campaign_id , True)

    elif 0 <= achieving_rate < 1 and spend < 0.8 * daily_budget:
        print('[handle_initial_bids][spend]: lower daily_budget')
        print('[handle_initial_bids][target]: lower daily_target')
        adjust_init_bids(keyword_insights_dict_list, 1.25)
        modify_opt_result_db(campaign_id , False)
        return

    elif 0 <= achieving_rate < 1 and spend >= 1.5 * daily_budget:
        print('[handle_initial_bids][spend]: over daily_budget')
        print('[handle_initial_bids][target]: lower daily_target')
        adjust_init_bids(keyword_insights_dict_list, 0.75)
        modify_opt_result_db(campaign_id , True)


# In[9]:


if __name__=="__main__":
    start_time = datetime.datetime.now()
    print('current time: ', start_time)
    optimize_performance_campaign()
    optimize_branding_campaign()
    print(datetime.datetime.now() - start_time)


# In[1]:


# !jupyter nbconvert --to script gsn_externals.ipynb


# In[11]:


# [keyword_insights for keyword_insights in keyword_insights_dict_list]


# In[ ]:




