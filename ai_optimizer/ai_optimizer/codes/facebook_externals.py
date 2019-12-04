#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import requests
import time
import pytz
import datetime
import math
import random
import pandas as pd
from copy import deepcopy
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.targeting import Targeting
from facebook_business.adobjects.ad import Ad
from facebook_business.api import FacebookAdsApi
import facebook_business.adobjects.campaign as facebook_business_campaign

from bid_operator import revert_bid_amount
import database_controller
from facebook_datacollector import Campaigns
from facebook_datacollector import DatePreset
from facebook_adapter import FacebookCampaignAdapter
import facebook_currency_handler as fb_currency_handler
import facebook_lookalike_audience as lookalike_audience
import facebook_ai_behavior_log as ai_logger
import facebook_adset_controller as adset_controller

import adgeek_permission as permission

IS_DEBUG = False #debug mode will not modify anything

DATABASE = 'dev_facebook_test'
DATE = datetime.datetime.now().date()#-datetime.timedelta(1)
ACTION_BOUNDARY = 0.8
ACTION_DICT = {'bid': AdSet.Field.bid_amount,
               'age': AdSet.Field.targeting, 'interest': AdSet.Field.targeting}

BRANDING_CAMPAIGN_LIST = [
    'THRUPLAY', 'LINK_CLICKS', 'ALL_CLICKS', 'VIDEO_VIEWS', 'REACH', 'POST_ENGAGEMENT', 'PAGE_LIKES', 'LANDING_PAGE_VIEW']
PERFORMANCE_CAMPAIGN_LIST = [
    'PURCHASE', 'MESSAGES', 'SEARCH', 'INITIATE_CHECKOUT', 'LEAD_WEBSITE', 'PURCHASES', 'ADD_TO_WISHLIST', 'VIEW_CONTENT', 'ADD_PAYMENT_INFO', 'COMPLETE_REGISTRATION', 'CONVERSIONS', 'LEAD_GENERATION', 'ADD_TO_CART']
CUSTOM_CONVERSION_CAMPAIGN_LIST = [
    'CUSTOM', 'CONVERSIONS'
]

ADSET_MAX_COUNT_CPA = 5
ADSET_MIN_COUNT = 3
ADSET_COPY_COUNT = 3

AI_ADSET_PREFIX = '_AI_'


def update_interest(adset_id=None, adset_params=None):
    adset = AdSet(adset_id)
    update_response = adset.update(adset_params)
    remote_update_response = adset.remote_update(
        params={'status': 'PAUSED', }
    )
    return


def update_status(adset_id, status=AdSet.Status.active):
    if IS_DEBUG:
        return 
    if status == AdSet.Status.paused:
        ai_logger.save_adset_behavior(adset_id, ai_logger.BehaviorType.CLOSE)
        
    adset = AdSet(adset_id)
    adset[AdSet.Field.status] = status
    adset.remote_update()

def update_daily_min_spend_target(adset_id):
    if IS_DEBUG:
        return 
    adset = AdSet(adset_id)
    adset[AdSet.Field.status] = status
    adset.remote_update()

    
def get_sorted_adset(campaign_id,):
    df = database_fb.retrieve('score', campaign_id)
    if df.empty:
        print('[get_sorted_adset] Error, no adset score')
        return []
    else:
        df_today = df.sort_values( by=['score'], ascending=False)
        print('[get_sorted_adset] df_today' , df_today )
        return df_today['adset_id'].unique().tolist()

def split_adset_list(adset_list):
    import math
    half = math.ceil(len(adset_list) / 2)
    return adset_list[:ADSET_COPY_COUNT], adset_list[half:]


def is_contain_copy_string(adset_name):
    return (AI_ADSET_PREFIX in adset_name)

def is_contain_rt_string(adset_name):
    return ('RT_' in adset_name)

def is_contain_lookalike_string(adset_name):
    return ('Look-a-like' in adset_name)


def modify_opt_result_db(campaign_id, is_optimized):
    #get date
    opt_date = datetime.date.today()
    database_fb.update("campaign_target", {"is_optimized": is_optimized, "optimized_date": opt_date }, campaign_id=campaign_id)

def optimize_performance_campaign(campaign_id):
    
    print('[optimize_performance_campaign] campaign ',campaign_id)
    df = database_fb.get_one_campaign(campaign_id)
    is_smart_spending = eval(df['is_smart_spending'].iloc[0])
    is_target_suggest = eval(df['is_target_suggest'].iloc[0])
    is_lookalike = eval(df['is_lookalike'].iloc[0])
    current_flight = ( datetime.date.today()-df['ai_start_date'].iloc[0] ).days
    period = df['period'].iloc[0]
    flight_process = current_flight / period
    destination_type = df['destination_type'].iloc[0]
    daily_charge = df['daily_charge'].iloc[0]
    campaign_daily_budget = df['daily_budget'].iloc[0]
    campaign_instance = Campaigns(campaign_id)
    
    day_dict = campaign_instance.generate_info(date_preset=DatePreset.yesterday)
    
    # this lifetime means ai_start_date and ai_stop_date; 
    lifetime_dict = campaign_instance.generate_info(date_preset=DatePreset.lifetime)
    day_dict['target'] = day_dict.pop('action')
    lifetime_dict['target'] = lifetime_dict.pop('action')
    lifetime_target = int( lifetime_dict['target'] )
    
    #get setting of destination and spending
    ai_setting_spend_cap = int(df['ai_spend_cap'])
    ai_setting_destination_count = int(df['destination'])
    ai_setting_cost_per_result = ai_setting_spend_cap/ai_setting_destination_count
    print('[optimize_performance_campaign] ai_setting_destination_count:' ,ai_setting_destination_count, ' ai_setting_spend_cap:', ai_setting_spend_cap, ' ai_setting_cost_per_result:',ai_setting_cost_per_result)
    
    
    # good enough
    if lifetime_target > df['destination'].iloc[0]:
        modify_opt_result_db(campaign_id, "False")
        print('[optimize_performance_campaign] lifetime good enough')
        return    

    #compute achieving_rate
    target = 0 # get by insight
    if 'target' in day_dict:
        target = int(day_dict['target'])
    
    achieving_rate = lifetime_target / (df['destination'].iloc[0] * flight_process + 5)
    print('[achieving rate]', achieving_rate, ' current_target', lifetime_target, ' destined_target', (df['destination'].iloc[0] * flight_process))

    if achieving_rate > ACTION_BOUNDARY and achieving_rate < 1:
        print('[optimize_performance_campaign] 0.8 < achieving_rate < 1')
    elif achieving_rate < ACTION_BOUNDARY:
        # update bid for original existed adset
        print('[optimize_performance_campaign] campaign_daily_budget', campaign_daily_budget)
        if not day_dict.get('spend'):
            print('[optimize_performance_campaign] no spend value')            
            return
        yesterday_spend = float(day_dict.get('spend'))
        if campaign_daily_budget and yesterday_spend and (yesterday_spend <= campaign_daily_budget * 0.8):
            print('[optimize_performance_campaign] yesterday_spend not enough:', yesterday_spend)            
            if not IS_DEBUG:
                database_fb.update_init_bid(campaign_id, update_ratio=1.1)
        else:
            print('[optimize_performance_campaign] yesterday_spend is enough, no need to up bidding')
    else: # good enough, not to do anything
        print('[optimize_performance_campaign] good enough, not to do anything')
        modify_opt_result_db(campaign_id , "False")
        return

    #not to generate suggestion for CPA campaign if adset count > ADSET_MAX_COUNT_CPA
    total_destination = df['destination'].iloc[0]
    ai_period = df['period'].iloc[0]

    adsets_active_list = campaign_instance.get_adsets_active()
    print('[optimize_performance_campaign] adsets_active_list:', adsets_active_list)
    if len(adsets_active_list) <= ADSET_MAX_COUNT_CPA:
        if len(adsets_active_list) > 0 and not IS_DEBUG: 
#         if len(adsets_active_list) > 0:
            #create one suggestion adset for CPA campaigin
            print('create one suggestion asset for CPA campaigin')        
            adset_id = adsets_active_list[0]
            if is_target_suggest:
                new_adset_id = adset_controller.make_performance_suggest_adset(campaign_id, adsets_active_list)
                if new_adset_id:
                    ai_logger.save_adset_behavior(new_adset_id, ai_logger.BehaviorType.CREATE)
                
            #create one lookalike adset for CPA campaigin
            print('create one lookalike asset for CPA campaigin')
            if is_lookalike:
                new_adset_id = adset_controller.make_performance_lookalike_adset(campaign_id, adsets_active_list)
                if new_adset_id:
                    ai_logger.save_adset_behavior(new_adset_id, ai_logger.BehaviorType.CREATE)
            
            modify_opt_result_db(campaign_id, "True")
            return

    #performance not to copy adset, just close adset
    adset_list = get_sorted_adset(campaign_id)
    adset_action_list = []
    for adset in adset_list:
        if str(adset) in adsets_active_list:
            adset_action_list.append(adset)
    
    adset_for_copy_list, adset_for_off_list = split_adset_list(adset_action_list)
    # current going adset is less than ADSET_MIN_COUNT, not to close any adset
    if len(adsets_active_list) <= ADSET_MIN_COUNT:
        adset_for_off_list = []
    
    print('[optimize_performance_campaign] adset_list',len(adset_list))
    print('[optimize_performance_campaign] adset_action_list',len(adset_action_list))
    print('[optimize_performance_campaign] adset_for_copy_list',len(adset_for_copy_list))
    print('[optimize_performance_campaign] adset_for_off_list',len(adset_for_off_list))

    for adset_id in adset_for_off_list:
        origin_adset_params = adset_controller.retrieve_origin_adset_params(adset_id)
        origin_name = origin_adset_params[AdSet.Field.name]
        if not is_contain_rt_string(origin_name):
            if adset_controller.is_adset_should_close(int(adset_id), ai_setting_cost_per_result):
                update_status(adset_id, status=AdSet.Status.paused)
    
    # optimize finish
    modify_opt_result_db(campaign_id, "True")
    
def optimize_branding_campaign(campaign_id):
    print('[optimize_branding_campaign] campaign ',campaign_id)
    df = database_fb.get_one_campaign(campaign_id)
    # charge_type attribute of first row
    is_smart_spending = eval(df['is_smart_spending'].iloc[0])
    is_target_suggest = eval(df['is_target_suggest'].iloc[0])
    is_lookalike = eval(df['is_lookalike'].iloc[0])
    current_flight = ( datetime.date.today()-df['ai_start_date'].iloc[0] ).days
    period = df['period'].iloc[0]
    flight_process = current_flight / period
    destination_type = df['destination_type'].iloc[0]
    daily_charge = df['daily_charge'].iloc[0]
    campaign_daily_budget = df['daily_budget'].iloc[0]
    campaign_instance = Campaigns(campaign_id)
    
    #get setting of destination and spending
    ai_setting_spend_cap = int(df['ai_spend_cap'])
    ai_setting_destination_count = int(df['destination'])
    ai_setting_cost_per_result = ai_setting_spend_cap/ai_setting_destination_count
    print('[optimize_branding_campaign] ai_setting_destination_count:' ,ai_setting_destination_count, ' ai_setting_spend_cap:', ai_setting_spend_cap, ' ai_setting_cost_per_result:',ai_setting_cost_per_result)
    
    day_dict = campaign_instance.generate_info(date_preset=DatePreset.yesterday)
    # this lifetime means ai_start_date and ai_stop_date; 
    lifetime_dict = campaign_instance.generate_info(date_preset=DatePreset.lifetime)
    day_dict['target'] = day_dict.pop('action')
    lifetime_dict['target'] = lifetime_dict.pop('action')
    lifetime_target = int( lifetime_dict['target'] )
    if lifetime_target > df['destination'].iloc[0]:
        print('[optimize_branding_campaign] good enough, not to do anything')        
        modify_opt_result_db(campaign_id, "False")
        return    
    
    #compute achieving_rate
    target = 0 # get by insight
    if 'target' in day_dict:
        target = int(day_dict['target'])
    
    achieving_rate = lifetime_target / (df['destination'].iloc[0] * flight_process)
    print('[achieving rate]', achieving_rate, ' current_target', lifetime_target, ' destined_target', (df['destination'].iloc[0] * flight_process))

    if achieving_rate > ACTION_BOUNDARY and achieving_rate < 1:
        print('[optimize_branding_campaign] 0.8 < achieving_rate < 1')
    elif achieving_rate < ACTION_BOUNDARY:
        # update bid for original existed adset
        print('[optimize_branding_campaign] campaign_daily_budget', campaign_daily_budget)
        if not day_dict.get('spend'):
            print('[optimize_performance_campaign] no spend value')            
            return
        yesterday_spend = float(day_dict.get('spend'))
        if campaign_daily_budget and yesterday_spend and (yesterday_spend <= campaign_daily_budget * 0.8):
            print('[optimize_branding_campaign] yesterday_spend not enough:', yesterday_spend)            
            if not IS_DEBUG:
                database_fb.update_init_bid(campaign_id, update_ratio=1.1)
        else:
            print('[optimize_branding_campaign] yesterday_spend is enough, no need to up bidding')
    else: # good enough, not to do anything
        print('[optimize_branding_campaign] good enough, not to do anything')
        modify_opt_result_db(campaign_id , "False")
        return

    # current going adset is less than ADSET_MIN_COUNT, not to close any adset
    adsets_active_list = campaign_instance.get_adsets_active()
    print('[optimize_branding_campaign] adsets_active_list:', adsets_active_list)
    
    adset_list = get_sorted_adset(campaign_id,)
    adset_action_list = []
    for adset in adset_list:
        if str(adset) in adsets_active_list:
            adset_action_list.append(adset)
    adset_for_copy_list, adset_for_off_list = split_adset_list(adset_action_list)
    # current going adset is less than ADSET_MIN_COUNT, not to close any adset
    if len(adsets_active_list) <= ADSET_MIN_COUNT:
        adset_for_off_list = []
    
    print('[optimize_branding_campaign] adset_list',len(adset_list))
    print('[optimize_branding_campaign] adset_action_list',len(adset_action_list))
    print('[optimize_branding_campaign] adset_for_copy_list',len(adset_for_copy_list))
    print('[optimize_branding_campaign] adset_for_off_list',len(adset_for_off_list))

    for adset_id in adset_for_off_list:
        origin_adset_params = adset_controller.retrieve_origin_adset_params(adset_id)
        origin_name = origin_adset_params[AdSet.Field.name]
        if not is_contain_rt_string(origin_name):
            if adset_controller.is_adset_should_close(int(adset_id), ai_setting_cost_per_result):
                update_status(adset_id, status=AdSet.Status.paused)
    
    # get ready to duplicate
    actions = {'bid': None, 'age': list(), 'interest': None}
    actions_list = list()
        
    #get adset bid for this campaign
    fb_adapter = FacebookCampaignAdapter( campaign_id, database_fb )
    fb_adapter.retrieve_campaign_attribute()
    
    for adset_id in adset_for_copy_list:
        # bid adjust
        bid = fb_adapter.init_bid_dict.get(int(adset_id))
        
        #error handle: the adset did not have score
        if bid is None:
            print('[optimize_branding_campaign] adset bid is None')
            break

        bid = fb_currency_handler.get_proper_bid(campaign_id, bid)

        actions.update({'bid': bid})
        origin_adset_params = adset_controller.retrieve_origin_adset_params(adset_id)
        origin_adset_params[AdSet.Field.id] = None
        origin_name = origin_adset_params[AdSet.Field.name]
        
#         # optimize by daily_min_spend_target
#         if 'daily_min_spend_target' in origin_adset_params:
#             new_daily_min_spend_target = int( int(origin_adset_params[AdSet.Field.daily_min_spend_target]) * 1.1)
#             actions.update({'daily_min_spend_target':  new_daily_min_spend_target })
#         else:
#             new_daily_min_spend_target = int(  campaign_daily_budget / len(adset_for_copy_list))
#             actions.update({'daily_min_spend_target':  new_daily_min_spend_target })
        
        adset_max = origin_adset_params[AdSet.Field.targeting]["age_max"]
        adset_min = origin_adset_params[AdSet.Field.targeting]["age_min"]

        try:
            actions['age'][0] = str(adset_min) + '-' + str(adset_max)
            actions.update({'interest': origin_interest['interests'][0]})
            
        except:
            actions['age'].append(str(adset_min) + '-' + str(adset_max))
            actions.update({'interest': None})
            
        # whether to split age or copy adset names with 'copy'
        if is_contain_copy_string(origin_name):
            print('[optimize_branding_campaign] not to copy the copied adset')
        else:
            # for CPC case without COPY string
            interval = 2
            age_interval = math.ceil((adset_max-adset_min) / interval)
            for i in range(interval):
                current_adset_min = adset_min
                current_adset_max = current_adset_min + age_interval
                actions['age'][0] = str(current_adset_min) + '-' + str(current_adset_max)
                adset_min = current_adset_max
                actions_copy = deepcopy(actions)
                copy_result_new_adset_id = adset_controller.copy_branding_adset(campaign_id, adset_id, actions_copy, origin_adset_params)
                if copy_result_new_adset_id:
                    ai_logger.save_adset_behavior(copy_result_new_adset_id, ai_logger.BehaviorType.COPY)
                
    modify_opt_result_db(campaign_id, "True")    
    
    
def optimize_campaign(campaign_id):
    print('[optimize_campaign] campaign_id', campaign_id)
    df = database_fb.get_one_campaign(campaign_id)
    destination_type = df['destination_type'].iloc[0]
    account_id = df['account_id'].iloc[0]
    permission.init_facebook_api(account_id)
    
    campaign_name , campaign_fb_status = get_campaign_name_status(campaign_id)
    print(campaign_id, campaign_fb_status, campaign_name)
    if campaign_fb_status == 'ACTIVE':
        print('[optimize_campaign] destination_type', destination_type)
        if destination_type in PERFORMANCE_CAMPAIGN_LIST:
            optimize_performance_campaign(campaign_id)
        elif destination_type in BRANDING_CAMPAIGN_LIST:
            optimize_branding_campaign(campaign_id)
        elif destination_type in CUSTOM_CONVERSION_CAMPAIGN_LIST:
            optimize_performance_campaign(campaign_id)
        else:
            print('[optimize_campaign] error, not optimize')


# In[ ]:


def get_campaign_name_status(campaign_id):
    this_campaign = facebook_business_campaign.Campaign( campaign_id).api_get(fields=["status", "name"])
    return this_campaign.get('name'), this_campaign.get('status')


# In[ ]:


if __name__ == '__main__':
    current_time = datetime.datetime.now()
    global database_fb
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    print('[facebook_externals] current_time:', current_time)
    campaign_not_opted_list = database_fb.get_not_opted_campaign().to_dict('records')
    
    print('df_not_opted len:', len(campaign_not_opted_list))
    print(campaign_not_opted_list)
    for campaign in campaign_not_opted_list:
        account_id = campaign.get("account_id")
        campaign_id = campaign.get("campaign_id")        
        destination = campaign.get("destination")
        destination_type = campaign.get("destination_type")
        ai_start_date = campaign.get("ai_start_date")
        ai_stop_date = campaign.get("ai_stop_date")
        custom_conversion_id = campaign.get("custom_conversion_id")

        optimize_campaign(campaign_id)
        print('==========next campaign========')
    print(datetime.datetime.now().date(), '==================!!facebook_externals.py finish!!=======================')
    
#     optimize_campaign(23843642051100463)


# In[ ]:


#nate test
# adset_controller.make_suggest_adset(23843604240180098,23843467729120098)


# In[ ]:


# !jupyter nbconvert --to script facebook_externals.ipynb


# In[ ]:


# %%time
# global database_fb
# db = database_controller.Database()
# database_fb = database_controller.FB(db)
# not_opt_list = [23843488842640474]
# for campaign_id in not_opt_list:
#     optimize_campaign(campaign_id)


# In[ ]:


# global database_fb
# db = database_controller.Database()
# database_fb = database_controller.FB(db)
# account_id = 2371372156415755
# campaign_id = 23843426278230073
# permission.init_facebook_api(account_id)
# optimize_branding_campaign(campaign_id)


# In[ ]:




