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
from facebook_business.api import FacebookAdsApi
import facebook_business.adobjects.campaign as facebook_business_campaign
import facebook_business.adobjects.ad as facebook_business_ad

from bid_operator import revert_bid_amount
import mysql_adactivity_save
from facebook_datacollector import Campaigns
from facebook_datacollector import DatePreset
from facebook_adapter import FacebookCampaignAdapter
import facebook_currency_handler as fb_currency_handler
import facebook_lookalike_audience as lookalike_audience
import facebook_campaign_suggestion as campaign_suggestion

my_access_token = 'EAANoD9I4obMBALrHTgMWgRujnWcZA3ZB823phs6ynDDtQxnzIZASyRQZCHfr5soXBZA7NM9Dc4j9O8FtnlIzxiPCsYt4tmPQ6ZAT3yJLPuYQqjnWZBWX5dsOVzNhEqsHYj1jVJ3RAVVueW7RSxRDbNXKvK3W23dcAjNMjxIjQGIOgZDZD'
ADSET_COPY_STRING = ' - Copy'
AI_ADSET_PREFIX = '_AI_'
IS_DEBUG = False #debug mode will not modify anything

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
    AdSet.Field.daily_min_spend_target,
    #     AdSet.Field.use_new_app_click
]


# In[2]:



def make_adset(adset_params):
    account_id = adset_params[AdSet.Field.account_id]
    new_adset = AdSet(parent_id='act_{}'.format(account_id))
    new_adset.update(adset_params)
    new_adset.remote_create(params={'status': 'ACTIVE', })
    return new_adset[AdSet.Field.id]

def retrieve_origin_adset_params(origin_adset_id):
    origin_adset = AdSet(fbid=origin_adset_id)
    origin_adset_params = origin_adset.remote_read(fields=FIELDS)
    return origin_adset_params

def get_ad_id_list(adset_id):
    ad_id_list = list()
    adset = AdSet(adset_id)
    ads = adset.get_ads(fields=[AdSet.Field.id])
    for ad in ads:
        ad_id_list.append(ad['id'])
    return ad_id_list


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


# In[3]:




def make_performance_suggest_adset(original_adset_id, campaign_id): 
    
    saved_suggest_id_name_dic = campaign_suggestion.get_suggestion_not_used(campaign_id)
    print('[make_suggest_adset] saved_suggest_id_name_dic', saved_suggest_id_name_dic)
    
    if not saved_suggest_id_name_dic:
        print('[make_suggest_adset] saved_suggest_id_name_dic not exist')
        return
        
    suggest_id_list = []
    suggest_name_list = []
    for saved_suggest_id in saved_suggest_id_name_dic:
        suggest_id_list.append(saved_suggest_id)
        
    #ramdom choose the first 3 interest    
    random.shuffle(suggest_id_list)
    PICK_COUNT = 3
    suggest_id_list = suggest_id_list[:PICK_COUNT]
    for suggest_id in suggest_id_list:
        suggest_name_list.append(saved_suggest_id_name_dic.get(suggest_id))
    
    print('[make_suggest_adset] ramdom choose:', suggest_id_list, suggest_name_list)
    
    new_adset_params = retrieve_origin_adset_params(original_adset_id)
    print('[make_suggest_adset] new_adset_params', new_adset_params)
    
    if new_adset_params[AdSet.Field.targeting].get("flexible_spec") == None:
        print('[make_suggest_adset] no flexible_spec')
        return

    suggestion_full_name =  '__'.join(suggest_name_list)
    new_adset_params[AdSet.Field.name] = "AI__" + str(datetime.datetime.now().date()) + '_' + suggestion_full_name

    suggest_group_list = []
    for i in range(len(suggest_id_list)):
        this_dic = {"id": suggest_id_list[i], "name": suggest_name_list[i]}
        suggest_group_list.append(this_dic)
    
    if new_adset_params[AdSet.Field.targeting].get("custom_audiences"): 
        print('[make_suggest_adset] remove custom_audiences when add suggestion adset')
        del new_adset_params[AdSet.Field.targeting]['custom_audiences']
    
    if new_adset_params.get("daily_min_spend_target"):
        print('[make_suggest_adset] remove daily_min_spend_target when add suggestion adset')
        del new_adset_params['daily_min_spend_target']
    
    if new_adset_params.get("daily_spend_cap"):
        print('[make_suggest_adset] remove daily_spend_cap when add suggestion adset')
        del new_adset_params['daily_spend_cap']

    new_adset_params[AdSet.Field.targeting]["flexible_spec"][0]['interests'] = suggest_group_list

    print('[make_suggest_adset] new_adset_params after process',new_adset_params)
    new_adset_params[AdSet.Field.id] = None
    new_adset_id = copy_adset_new_target(new_adset_params, original_adset_id)
    return new_adset_id


# ##not use now , just backup
# def make_suggest_adset_by_account_suggestion(original_adset_id): 
#     suggestion_id, suggestion_name = get_suggestion_target_by_adset(original_adset_id)
#     if suggestion_id is None:
#         print('[make_suggest_adset] error')
#         return 
    
#     print('[make_suggest_adset] pick this suggestion:',suggestion_id, suggestion_name)
    
#     new_adset_params = retrieve_origin_adset_params(original_adset_id)
#     print(new_adset_params)
#     new_adset_params[AdSet.Field.name] = suggestion_name + "_Target_AI"

#     interest_pair = {
#             "interests":[{
#                 "id": suggestion_id,
#                 "name": suggestion_name,
#             }]
#         }
#     if new_adset_params[AdSet.Field.targeting].get("flexible_spec") == None:
#         print('[make_suggest_adset_by_account_suggestion] no flexible_spec')
        
#     new_adset_params[AdSet.Field.targeting]["flexible_spec"] = interest_pair

#     print('[make_suggest_adset] new_adset_params',new_adset_params)
#     original_adset_id = new_adset_params[AdSet.Field.id]
#     new_adset_params[AdSet.Field.id] = None
#     new_adset_id = copy_adset_new_target(new_adset_params, original_adset_id)
#     return new_adset_id


# In[4]:


def ad_name_remove_copy_string(adset_id):
    this_ad = facebook_business_ad.Ad(adset_id).remote_read(fields=["name"])
    this_ad_name = this_ad.get('name')
    index = this_ad_name.find(ADSET_COPY_STRING)
    if index > -1:
        remove_copy_name = this_ad_name[:index]
        this_ad.remote_update(
            params={'name': remove_copy_name, }
        )
    

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


def copy_adset_new_target(new_adset_params, original_adset_id):
    new_adset_id = -1
    try:
        new_adset_id = make_adset(new_adset_params)
        print('[copy_adset_new_target] make_adset success, original_adset_id', original_adset_id, ' new_adset_id', new_adset_id)
        time.sleep(10)
        ad_id_list = get_ad_id_list(original_adset_id)
        print('ad_id_list', ad_id_list)

        for ad_id in ad_id_list:
            result_message = assign_copied_ad_to_new_adset(new_adset_id=new_adset_id, ad_id=ad_id)
            print('[copy_adset_new_target] result_message', result_message)
            if 'copied_ad_id' in result_message:
                copied_ad_id = json.loads(result_message).get('copied_ad_id')
                ad_name_remove_copy_string(copied_ad_id)

        return new_adset_id
            
    except Exception as error:
        print('[copy_adset_new_target] this adset is not existed anymore, error:', error)


# In[5]:



def copy_branding_adset(adset_id, actions, adset_params=None):
    
    new_adset_params = adset_params
    origin_adset_name = adset_params[AdSet.Field.name]
    new_adset_params[AdSet.Field.id] = None
    
    # Generate new adset name
    new_adset_name = ''
    index = origin_adset_name.find(AI_ADSET_PREFIX)
    if index > 0:
        new_adset_name = origin_adset_name[:index]  + AI_ADSET_PREFIX + str(datetime.datetime.now().date()) 
    else:
        new_adset_name = origin_adset_name + AI_ADSET_PREFIX + str(datetime.datetime.now().date())
    
    for i, action in enumerate(actions.keys()):
        if action == 'bid':
            new_adset_params[ACTION_DICT[action]] = math.floor( revert_bid_amount(actions[action]) )  # for bid

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
    
    new_adset_id = copy_adset_new_target(new_adset_params, original_adset_id)
    return new_adset_id
            


# In[6]:


def make_performance_lookalike_adset(campaign_id, adsets_active_list):
    original_adset_id = adsets_active_list[0]
    new_adset_params = retrieve_origin_adset_params(original_adset_id)
    new_adset_params.pop("id")
    ad_id_list = get_ad_id_list(original_adset_id)

    targeting = new_adset_params["targeting"]
    targeting.pop("flexible_spec", None)

    lookalike_audience_dict = lookalike_audience.get_lookalike_audience_id(campaign_id)
    print('[make_performance_lookalike_adset] lookalike_audience_dict:', lookalike_audience_dict)
    
    if not lookalike_audience_dict:
        print('[make_lookalike_adset]: lookalike_audience_dict None')
        return
    if not any(lookalike_audience_dict):
        print('[make_lookalike_adset]: all lookalike is in adset.')
        return
    
    # Pick first lookalike audience

    lookalike_audience_id = list(lookalike_audience_dict.values())[0]
    targeting["custom_audiences"] = [{"id": lookalike_audience_id}]
    new_adset_params["name"] = "AI__" + "Look-a-like Custom {}".format(list(lookalike_audience_dict.keys())[0])
    print('==================')
    print('new_adset_params:', new_adset_params)
    
    new_adset_id = copy_adset_new_target(new_adset_params, original_adset_id)
    if new_adset_id:
        lookalike_audience.modify_result_db(campaign_id, lookalike_audience_id, True)
    return new_adset_id
    

