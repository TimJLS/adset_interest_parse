#!/usr/bin/env python
# coding: utf-8

# In[1]:


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

from bid_operator import revert_bid_amount
import mysql_adactivity_save
from facebook_datacollector import Campaigns
from facebook_datacollector import DatePreset
from facebook_adapter import FacebookCampaignAdapter
IS_DEBUG = False #debug mode will not modify anything


my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBALrHTgMWgRujnWcZA3ZB823phs6ynDDtQxnzIZASyRQZCHfr5soXBZA7NM9Dc4j9O8FtnlIzxiPCsYt4tmPQ6ZAT3yJLPuYQqjnWZBWX5dsOVzNhEqsHYj1jVJ3RAVVueW7RSxRDbNXKvK3W23dcAjNMjxIjQGIOgZDZD'
tim_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'
# 'EAANoD9I4obMBAHukuWuiyeNxAPGEojU982JmGHJP1MsdM03H3gY3EVQj5G3gzZCq7KECX0lOi87ZCGKZA5hy1INxZBhD6azH8oYHICQ1BhZAojC50zjgUa54f9R2VInhLGpHGXl6F1VWltmRK6LeF5kRkDvZC4lkZCzSU4II1gJ1NoC0SaiS6D6piSp2rUTqPXTtASZBRfy5tbscOhfXDSxyai7IAaeCFb5xRBH4QsRm8wZDZD'

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

DATABASE = 'dev_facebook_test'
DATE = datetime.datetime.now().date()
ACTION_BOUNDARY = 0.8
ACTION_DICT = {'bid': AdSet.Field.bid_amount,
               'age': AdSet.Field.targeting, 'interest': AdSet.Field.targeting}

BRANDING_CAMPAIGN_LIST = ['LINK_CLICKS', 'ALL_CLICKS','VIDEO_VIEWS', 'VIDEO_VIEW', 'REACH', 'POST_ENGAGEMENT', 'PAGE_LIKES', 'LANDING_PAGE_VIEW']
PERFORMANCE_CAMPAIGN_LIST = ['CONVERSIONS', 'LEAD_GENERATION', 'ADD_TO_CART']

ADSET_COPY_COUNT = 3
ADSET_MIN_COUNT = 3

FIELDS = [
    AdSet.Field.id,
    AdSet.Field.account_id,
    AdSet.Field.adlabels,
    AdSet.Field.adset_schedule,
    AdSet.Field.attribution_spec,
    AdSet.Field.bid_amount,
    AdSet.Field.bid_info,
    AdSet.Field.bid_strategy,
    AdSet.Field.billing_event,
    AdSet.Field.budget_remaining,
    AdSet.Field.campaign_id,
    AdSet.Field.configured_status,
    AdSet.Field.created_time,
    AdSet.Field.creative_sequence,
    AdSet.Field.daily_budget,
    AdSet.Field.daily_min_spend_target,
    AdSet.Field.daily_spend_cap,
    AdSet.Field.destination_type,
    AdSet.Field.effective_status,
    AdSet.Field.end_time,
    AdSet.Field.frequency_control_specs,
    AdSet.Field.instagram_actor_id,
    AdSet.Field.is_dynamic_creative,
    AdSet.Field.issues_info,
    AdSet.Field.lifetime_budget,
    AdSet.Field.lifetime_imps,
    AdSet.Field.lifetime_min_spend_target,
    AdSet.Field.lifetime_spend_cap,
    AdSet.Field.name,
    AdSet.Field.optimization_goal,
    AdSet.Field.pacing_type,
    AdSet.Field.promoted_object,
    AdSet.Field.recommendations,
    AdSet.Field.recurring_budget_semantics,
    AdSet.Field.rf_prediction_id,
    AdSet.Field.start_time,
    AdSet.Field.status,
    AdSet.Field.targeting,
    AdSet.Field.time_based_ad_rotation_id_blocks,
    AdSet.Field.time_based_ad_rotation_intervals,
    AdSet.Field.updated_time,
    AdSet.Field.daily_min_spend_target,
    #     AdSet.Field.use_new_app_click
]


def copy_adset_new_target(new_adset_params, original_adset_id):
    new_adset_id = -1
    try:
        new_adset_id = make_adset(new_adset_params)
        print('[copy_adset_new_target] make_adset success, original_adset_id', original_adset_id, ' new_adset_id', new_adset_id)
        time.sleep(10)
        ad_id_list = get_ad_id_list(original_adset_id)
        for ad_id in ad_id_list:
            result_message = assign_copied_ad_to_new_adset(new_adset_id=new_adset_id, ad_id=ad_id)
            print('[copy_adset_new_target] result_message', result_message)
            
    except Exception as error:
        print('[copy_adset_new_target] this adset is not existed anymore, error:', error)

def make_suggest_adset(original_adset_id): 
    suggestion_id, suggestion_name = get_suggestion_target_by_adset(original_adset_id)
    if suggestion_id is None:
        print('[make_suggest_adset] error')
        return 
    
    print('[make_suggest_adset] pick this suggestion:',suggestion_id, suggestion_name)
    
    new_adset_params = retrieve_origin_adset_params(original_adset_id)
    print(new_adset_params)
    new_adset_params[AdSet.Field.name] = suggestion_name + "_Target_AI"

    interest_pair = {
            "interests":[{
                "id": suggestion_id,
                "name": suggestion_name,
            }]
        }
    new_adset_params[AdSet.Field.targeting]["flexible_spec"] = interest_pair

    print('[make_suggest_adset] new_adset_params',new_adset_params)
    original_adset_id = new_adset_params[AdSet.Field.id]
    new_adset_params[AdSet.Field.id] = None
    copy_adset_new_target(new_adset_params, original_adset_id)



def get_account_id_by_adset(adset_id):
    this_adsets = AdSet( adset_id ).remote_read(fields=["account_id"])
    account_id = this_adsets.get('account_id')
    return account_id

def get_suggestion_target_by_adset(adset_id):
    account_id = get_account_id_by_adset(adset_id)
    
    mydb = mysql_adactivity_save.connectDB(DATABASE)
    mycursor = mydb.cursor()
    ### select
    sql = "SELECT suggestion_id, suggestion_name   FROM account_target_suggestion WHERE account_id={}".format(account_id)
    mycursor.execute(sql)
    target_suggestions = mycursor.fetchall()
#     for suggestions in target_suggestions:
#         print(suggestions)

    mydb.commit()
    mydb.close()
    print('')
    if len(target_suggestions) > 0:
        pick_index = random.randint(0, len(target_suggestions)-1)
        print('[get_suggestion_target_by_adset] pick:',target_suggestions[pick_index])
        return target_suggestions[pick_index]
    else:
        return None,None
    

def get_ad_id_list(adset_id):
    ad_id_list = list()
    adset = AdSet(adset_id)
    ads = adset.get_ads(fields=[AdSet.Field.id])
    for ad in ads:
        ad_id_list.append(ad[Ad.Field.id])
    return ad_id_list


def retrieve_origin_adset_params(origin_adset_id):
    origin_adset = AdSet(fbid=origin_adset_id)
    origin_adset_params = origin_adset.remote_read(fields=FIELDS)
    return origin_adset_params


def assign_copied_ad_to_new_adset(new_adset_id=None, ad_id=None):
    url = "https://graph.facebook.com/v3.2/{}/copies".format(ad_id)
    querystring = {
        "adset_id": "{}".format(new_adset_id),
        "status_option": "INHERITED_FROM_SOURCE"}
    headers = {
        'Authorization': "Bearer {}".format(my_access_token), }
    response = requests.request(
        "POST", url, headers=headers, params=querystring)
    return response.text


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
    adset = AdSet(adset_id)
    adset[AdSet.Field.status] = status
    adset.remote_update()

def update_daily_min_spend_target(adset_id):
    if IS_DEBUG:
        return 
    adset = AdSet(adset_id)
    adset[AdSet.Field.status] = status
    adset.remote_update()

    
def get_sorted_adset(campaign):
    mydb = mysql_adactivity_save.connectDB(DATABASE)

    df = pd.read_sql( "select * from adset_score where campaign_id=%s" % (campaign), con=mydb)
    adset_list = []

    if len(df) > 0:
        df_today = df[df.request_time.dt.date == DATE].sort_values( by=['score'], ascending=False)
        print('[get_sorted_adset] df_today' , df_today )
        
        if len(df_today) > 0:
            adset_list = df_today['adset_id'].unique().tolist()
        else:
            print('[get_sorted_adset] Error, no adset score today')    
    else:  
        print('[get_sorted_adset] Error, no adset score')

    print('[get_sorted_adset]  adset_list:', adset_list)
    mydb.close()
    return adset_list


def split_adset_list(adset_list):
    import math
    adset_list.sort(reverse=True)
    half = math.ceil(len(adset_list) / 2)
    return adset_list[:ADSET_COPY_COUNT], adset_list[half:]


def is_contain_copy_string(adset_name):
    return ('Copy' in adset_name)

def is_contain_rt_string(adset_name):
    return ('RT_' in adset_name)

def check_init_bid(init_bid):
    if init_bid > 100:
        bid = math.ceil(init_bid*1.1)
        return bid
    else:
        bid = init_bid + 1
        return bid


def copy_adset(adset_id, actions, adset_params=None):
    
    new_adset_params = adset_params
    origin_adset_name = adset_params[AdSet.Field.name]
    new_adset_params[AdSet.Field.id] = None
    
    # Generate new adset name
    new_adset_name = ''
    index = origin_adset_name.find("Copy")
    if index > 0:
        new_adset_name = origin_adset_name[:index]  + '_Copy_' + str(datetime.datetime.now().date()) 
    else:
        new_adset_name = origin_adset_name + '_Copy_' + str(datetime.datetime.now().date())
    
    for i, action in enumerate(actions.keys()):
        if action == 'bid':
            new_adset_params[ACTION_DICT[action]] = math.ceil( revert_bid_amount(actions[action]) )  # for bid

        elif action == 'age':
            age_list = actions[action][0].split('-')
            new_adset_params[AdSet.Field.targeting]["age_min"] = int(
                age_list[:1][0])
            new_adset_params[AdSet.Field.targeting]["age_max"] = int(
                age_list[1:][0])
            new_adset_params[AdSet.Field.name] = new_adset_name

#         elif action == 'interest':
# #             if actions[action] is None:
# #                 new_adset_params[AdSet.Field.targeting]["flexible_spec"] = None
# #             else:
#             new_adset_params[AdSet.Field.targeting]["flexible_spec"] = {
#                     "interests": [actions[action]]}

    new_adset_id = -1
    try:
        new_adset_id = make_adset(new_adset_params)
        print('[copy_adset] make_adset success, adset_id', adset_id, ' new_adset_id', new_adset_id)
        time.sleep(10)
        ad_id_list = get_ad_id_list(adset_id)
        for ad_id in ad_id_list:
            result_message = assign_copied_ad_to_new_adset(new_adset_id=new_adset_id, ad_id=ad_id)
            print('[copy_adset] result_message', result_message)
            
    except Exception as error:
        print('[copy_adset] this adset is not existed anymore, error:', error)


def make_adset(adset_params):
    account_id = adset_params[AdSet.Field.account_id]
    new_adset = AdSet(parent_id='act_{}'.format(account_id))
    new_adset.update(adset_params)
    new_adset.remote_create(params={'status': 'ACTIVE', })
    return new_adset[AdSet.Field.id]

def modify_opt_result_db(campaign_id, is_optimized):
    #get date
    opt_date = datetime.datetime.now()
    #insert to table date and Ture for is_opt
    sql = "update campaign_target set is_optimized = '{}', optimized_date = '{}' where campaign_id = {}".format(is_optimized, opt_date, campaign_id)
    mydb = mysql_adactivity_save.connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    mydb.commit()
    mydb.close()

def optimize_performance_campaign(campaign_id):
    
    print('[optimize_performance_campaign] campaign ',campaign_id)
    df = mysql_adactivity_save.get_campaign_target(campaign_id)

    charge_type = df['charge_type'].iloc[0]
    daily_charge = df['daily_charge'].iloc[0]
    campaign_daily_budget = df['daily_budget'].iloc[0]
    campaign_instance = Campaigns(campaign_id, charge_type)
    
    day_dict = campaign_instance.generate_campaign_info(date_preset=DatePreset.yesterday)
    # this lifetime means ai_start_date and ai_stop_date; 
    lifetime_dict = campaign_instance.generate_campaign_info(date_preset=DatePreset.lifetime)
    lifetime_target = int( lifetime_dict['target'] )
    
    # good enough
    if lifetime_target > df['destination'].iloc[0]:
        modify_opt_result_db(campaign_id, False)
        print('[optimize_performance_campaign] lifetime good enough')
        return    

    #compute achieving_rate
    target = 0 # get by insight
    if 'target' in day_dict:
        target = int(day_dict['target'])
    
    fb_adapter = FacebookCampaignAdapter(campaign_id)
    campaign_days_left = fb_adapter.campaign_days_left
    achieving_rate = target / daily_charge
    print('[achieving rate]', achieving_rate, ' target', target, ' daily_charge', daily_charge)
    

    if achieving_rate > ACTION_BOUNDARY and achieving_rate < 1:
        print('[optimize_performance_campaign] achieving_rate', achieving_rate)
    elif achieving_rate < ACTION_BOUNDARY:
        # update bid for original existed adset
        print('[optimize_performance_campaign] achieving_rate', achieving_rate)
        if not IS_DEBUG:
            mysql_adactivity_save.adjust_init_bid(campaign_id)
    else: # good enough, not to do anything
        print('[optimize_performance_campaign] good enough, not to do anything')
        modify_opt_result_db(campaign_id , False)
        return


    # current going adset is less than ADSET_MIN_COUNT, not to close any adset
    adsets_active_list = campaign_instance.get_adsets_active()
    print('[optimize_performance_campaign] adsets_active_list:', adsets_active_list)
    if len(adsets_active_list) <= ADSET_MIN_COUNT:
        if len(adsets_active_list) > 0 and not IS_DEBUG:
            #create one suggestion adset for CPA campaigin
            print('create one suggestion asset for CPA campaigin')        
            adset_id = adsets_active_list[0]
            make_suggest_adset(adset_id)
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
        origin_adset_params = retrieve_origin_adset_params(adset_id)
        origin_name = origin_adset_params[AdSet.Field.name]
        if not is_contain_rt_string(origin_name):
            update_status(adset_id, status=AdSet.Status.paused)
    
    # optimize finish
    modify_opt_result_db(campaign_id, True)
    
def optimize_branding_campaign(campaign_id):

    print('[optimize_branding_campaign] campaign ',campaign_id)
    df = mysql_adactivity_save.get_campaign_target(campaign_id)

    # charge_type attribute of first row
    charge_type = df['charge_type'].iloc[0]
    daily_charge = df['daily_charge'].iloc[0]
    campaign_daily_budget = df['daily_budget'].iloc[0]
    campaign_instance = Campaigns(campaign_id, charge_type)
    
    day_dict = campaign_instance.generate_campaign_info(date_preset=DatePreset.yesterday)
    # this lifetime means ai_start_date and ai_stop_date; 
    lifetime_dict = campaign_instance.generate_campaign_info(date_preset=DatePreset.lifetime)
    
    lifetime_target = int( lifetime_dict['target'] )
    if lifetime_target > df['destination'].iloc[0]:
        print('[optimize_branding_campaign] good enough, not to do anything')        
        modify_opt_result_db(campaign_id, False)
        return    
    
    #compute achieving_rate
    target = 0 # get by insight
    if 'target' in day_dict:
        target = int(day_dict['target'])
    
    fb_adapter = FacebookCampaignAdapter(campaign_id)
    campaign_days_left = fb_adapter.campaign_days_left
    achieving_rate = target / daily_charge
    print('[achieving rate]', achieving_rate, ' target', target, ' daily_charge', daily_charge)
    
    if achieving_rate > ACTION_BOUNDARY and achieving_rate < 1:
        print('[optimize_branding_campaign] achieving_rate', achieving_rate)
    elif achieving_rate < ACTION_BOUNDARY:
        # update bid for original existed adset
        print('[optimize_branding_campaign] achieving_rate', achieving_rate)
        if not IS_DEBUG:
            mysql_adactivity_save.adjust_init_bid(campaign_id)
    else: # good enough, not to do anything
        print('[optimize_branding_campaign] good enough, not to do anything')
        modify_opt_result_db(campaign_id , False)
        return

    # current going adset is less than ADSET_MIN_COUNT, not to close any adset
    adsets_active_list = campaign_instance.get_adsets_active()
    print('[optimize_branding_campaign] adsets_active_list:', adsets_active_list)
    
    adset_list = get_sorted_adset(campaign_id)
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
        origin_adset_params = retrieve_origin_adset_params(adset_id)
        origin_name = origin_adset_params[AdSet.Field.name]
        if not is_contain_rt_string(origin_name):
            update_status(adset_id, status=AdSet.Status.paused)
    
    # get ready to duplicate
    actions = {'bid': None, 'age': list(), 'interest': None}
    actions_list = list()
        
    #get adset bid for this campaign
    fb_adapter.retrieve_campaign_attribute()
    
    for adset_id in adset_for_copy_list:
        # bid adjust
        bid = fb_adapter.init_bid_dict.get(int(adset_id))
        
        #error handle: the adset did not have score
        if bid is None:
            print('[optimize_branding_campaign] adset bid is None')
            break

        bid = check_init_bid(bid)

        actions.update({'bid': bid})
        origin_adset_params = retrieve_origin_adset_params(adset_id)
        origin_adset_params[AdSet.Field.id] = None
        origin_name = origin_adset_params[AdSet.Field.name]
        
        # optimize by daily_min_spend_target
        if 'daily_min_spend_target' in origin_adset_params:
            new_daily_min_spend_target = int( int(origin_adset_params[AdSet.Field.daily_min_spend_target]) * 1.1)
            actions.update({'daily_min_spend_target':  new_daily_min_spend_target })
        else:
            new_daily_min_spend_target = int(  campaign_daily_budget / len(adset_for_copy_list))
            actions.update({'daily_min_spend_target':  new_daily_min_spend_target })
        
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
                copy_adset(adset_id, actions_copy, origin_adset_params)
                
    modify_opt_result_db(campaign_id, True)    
    
    
def optimize_campaign(campaign_id):
    df = mysql_adactivity_save.get_campaign_target(campaign_id)
    charge_type = df['charge_type'].iloc[0]
    
    print('[optimize_campaign] campaign_id', campaign_id, charge_type)
    if charge_type in PERFORMANCE_CAMPAIGN_LIST:
        optimize_performance_campaign(campaign_id)
    elif charge_type in BRANDING_CAMPAIGN_LIST:
        optimize_branding_campaign(campaign_id)
    else:
        print('[optimize_campaign] error, not optimize')


# In[2]:


if __name__ == '__main__':
    import index_collector_conversion_facebook
    current_time = datetime.datetime.now()
    print('[facebook_externals] current_time:', current_time)
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    df_not_opted = mysql_adactivity_save.get_campaigns_not_optimized()
    print('df_not_opted len:', len(df_not_opted))
    
    for campaign_id in df_not_opted.campaign_id.unique():
        optimize_campaign(campaign_id)
#     optimize_campaign(23843358370700576)


# In[ ]:




