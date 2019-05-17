#!/usr/bin/env python
# coding: utf-8

# In[8]:


# In[4]:


import json
import requests
import time
import pytz
import datetime
import math
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
    #     AdSet.Field.use_new_app_click
]





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


def get_sorted_adset(campaign):
    mydb = mysql_adactivity_save.connectDB(DATABASE)

    df = pd.read_sql( "select * from adset_score where campaign_id=%s" % (campaign), con=mydb)
    adset_list = []
    if len(df) > 0:
        df = df[df.request_time.dt.date == DATE].sort_values(
            by=['score'], ascending=False)
        adset_list = df['adset_id'].unique().tolist()
    else:  
        df_camp = mysql_adactivity_save.get_campaign_target()
        charge_type = df_camp['charge_type'].iloc[0]
        adset_list = Campaigns(campaign, charge_type).get_adsets()

    mydb.close()
    return adset_list


def split_adset_list(adset_list):
    import math
    adset_list.sort(reverse=True)
    half = math.ceil(len(adset_list) / 2)
    return adset_list[:half], adset_list[half:]


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
    #Nate if IS_DEBUG:
    #Nate    return

    new_adset_params = adset_params
    origin_adset_name = adset_params[AdSet.Field.name]
    new_adset_params[AdSet.Field.id] = None

    for i, action in enumerate(actions.keys()):
        if action == 'bid':
            new_adset_params[ACTION_DICT[action]] = math.ceil( revert_bid_amount(actions[action]) )  # for bid

        elif action == 'age':
            age_list = actions[action][0].split('-')
            new_adset_params[AdSet.Field.targeting]["age_min"] = int(
                age_list[:1][0])
            new_adset_params[AdSet.Field.targeting]["age_max"] = int(
                age_list[1:][0])
            new_adset_params[AdSet.Field.name] = origin_adset_name +' Copy - {}'.format(actions[action])

#         elif action == 'interest':
# #             if actions[action] is None:
# #                 new_adset_params[AdSet.Field.targeting]["flexible_spec"] = None
# #             else:
#             new_adset_params[AdSet.Field.targeting]["flexible_spec"] = {
#                     "interests": [actions[action]]}
    #print('Nate adset_id', adset_id)
    #print('Nate new_adset_params', new_adset_params)
    new_adset_id = -1
    try:
        new_adset_id = make_adset(new_adset_params)
        print('make_adset success, adset_id', adset_id)
    except:
        new_adset_id = make_adset(new_adset_params)
        print('this adset is not existed anymore, adset_id', adset_id)

    time.sleep(10)
    ad_id_list = get_ad_id_list(adset_id)
    print('Nate new_adset_id', new_adset_id)
    for ad_id in ad_id_list:
        result_message = assign_copied_ad_to_new_adset(new_adset_id=new_adset_id, ad_id=ad_id)
        print('Nate result_message', result_message)


def make_adset(adset_params):
    account_id = adset_params[AdSet.Field.account_id]
    new_adset = AdSet(parent_id='act_{}'.format(account_id))
    new_adset.update(adset_params)
    new_adset.remote_create(params={'status': 'ACTIVE', })
    return new_adset[AdSet.Field.id]

def modify_opt_result_db(campaign_id):
    #get date
    opt_date = datetime.datetime.now()
    #insert to table date and Ture for is_opt
    sql = "update campaign_target set optimized_date = '{}' where campaign_id = {}".format(opt_date, campaign_id)
    mydb = mysql_adactivity_save.connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    mydb.commit()
    mydb.close()

def handle_campaign_copy(campaign_id):
    
    print('[handle_campaign_copy] campaign ',campaign_id)
    df = mysql_adactivity_save.get_campaign_target(campaign_id)
    
    # charge_type attribute of first row
    charge_type = df['charge_type'].iloc[0]
    daily_charge = df['daily_charge'].iloc[0]
    day_dict = Campaigns(campaign_id, charge_type).generate_campaign_info(
        date_preset=DatePreset.yesterday)
    lifetime_dict = Campaigns(campaign_id, charge_type).generate_campaign_info(
        date_preset=DatePreset.lifetime)
#     print('[handle_campaign_copy] day_dict ',day_dict)
#     print('[handle_campaign_copy] lifetime_dict ',lifetime_dict)

    target = 0 # get by insight
    if 'target' in day_dict:
        target = int(day_dict['target'])
    
    fb_adapter = FacebookCampaignAdapter(campaign_id)
    campaign_days_left = fb_adapter.campaign_days_left
    achieving_rate = target / daily_charge
    print('[campaign_id]', campaign_id, '[achieving rate]', achieving_rate, target, daily_charge)

    adset_list = get_sorted_adset(campaign_id)
    print('adset_list',len(adset_list))
    
    adset_for_copy_list, adset_for_off_list = split_adset_list(adset_list)
    # get ready to duplicate
    actions = {'bid': None, 'age': list(), 'interest': None}
    actions_list = list()

    is_adjust_bid = False
    if achieving_rate > ACTION_BOUNDARY and achieving_rate < 1:
        is_adjust_bid = False
    elif achieving_rate < ACTION_BOUNDARY:
        is_adjust_bid = True
    else: # good enough, not to do anything
        modify_opt_result_db(campaign_id)
        return
    
    # update bid for original existed adset
    if is_adjust_bid and not IS_DEBUG:
        mysql_adactivity_save.adjust_init_bid(campaign_id)
        
    is_performance_campaign = False
    is_split_age = False
    if charge_type == 'CONVERSIONS' or charge_type == 'ADD_TO_CART':
        is_performance_campaign = True
        is_split_age = False
    elif charge_type == 'LINK_CLICKS':
        is_performance_campaign = False
        is_split_age = True
    
    for adset_id in adset_for_off_list:
        
        origin_adset_params = retrieve_origin_adset_params(adset_id)
        
        origin_name = origin_adset_params[AdSet.Field.name]
        if not is_contain_rt_string(origin_name):
            update_status(adset_id, status=AdSet.Status.paused)
    
    print('[handle_campaign_copy] adset_for_copy_list',adset_for_copy_list)
    
    #get adset bid for this campaign
    fb_adapter.retrieve_campaign_attribute()
    
    for adset_id in adset_for_copy_list:
        # bid adjust

        bid = fb_adapter.init_bid_dict.get(int(adset_id))
        #error handle: the adset did not have score
        if bid is None:
            print('[handle_campaign_copy] adset bid is None')
            break

        if is_adjust_bid:
            bid = check_init_bid(bid)

        actions.update({'bid': bid})
        origin_adset_params = retrieve_origin_adset_params(adset_id)
        origin_adset_params[AdSet.Field.id] = None
        origin_name = origin_adset_params[AdSet.Field.name]
        # interest decision
#         try:
#             origin_interest = origin_adset_params[AdSet.Field.targeting]["flexible_spec"][0]
#         except:
#             origin_interest = None
        # min max age
        adset_max = origin_adset_params[AdSet.Field.targeting]["age_max"]
        adset_min = origin_adset_params[AdSet.Field.targeting]["age_min"]

        try:
            actions['age'][0] = str(adset_min) + '-' + str(adset_max)
            actions.update({'interest': origin_interest['interests'][0]})
            
        except:
            actions['age'].append(str(adset_min) + '-' + str(adset_max))
            actions.update({'interest': None})
            
        # whether to split age or copy adset names with 'copy'
        if is_performance_campaign or is_contain_copy_string(origin_name):
            # for CPA case or CPC with COPY string
            actions['age'] = list()
            actions['age'].append(str(adset_min) + '-' + str(adset_max))
            actions_copy = deepcopy(actions)
            copy_adset(adset_id, actions_copy, origin_adset_params)
        else:
            # for CPC case without COPY string
            if is_split_age:
                interval = 2
                age_interval = math.ceil((adset_max-adset_min) / interval)
                for i in range(interval):
                    current_adset_min = adset_min
                    current_adset_max = current_adset_min + age_interval
                    actions['age'][0] = str(current_adset_min) + '-' + str(current_adset_max)
                    adset_min = current_adset_max
                    actions_copy = deepcopy(actions)
                    copy_adset(adset_id, actions_copy, origin_adset_params)
            else:
                actions_copy = deepcopy(actions)
                actions_list.append(actions_copy)
                copy_adset(adset_id, actions_copy, origin_adset_params)
                
    modify_opt_result_db(campaign_id)


# In[2]:


if __name__ == '__main__':
    import index_collector_conversion_facebook
    current_time = datetime.datetime.now()
    print('[facebook_externals] current_time:', current_time)
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    df_is_running = mysql_adactivity_save.get_campaigns_not_optimized()
#     print('[facebook_externals] len:', len(df_is_running))
#     print('[facebook_externals] df_is_running', df_is_running)
       
    for campaign_id in df_is_running.campaign_id.unique():
        handle_campaign_copy(campaign_id)
#     handle_campaign_copy(23843419701490612)


# In[7]:





# In[ ]:




