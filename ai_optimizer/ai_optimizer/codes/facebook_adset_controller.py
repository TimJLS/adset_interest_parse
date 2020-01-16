#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import time
import datetime
import math
import random
import requests

from facebook_business.adobjects.adset import AdSet
import facebook_business.adobjects.ad as facebook_business_ad

from bid_operator import revert_bid_amount
import database_controller
import adgeek_permission as permission

import facebook_lookalike_audience as lookalike_audience
import facebook_campaign_suggestion as campaign_suggestion
import facebook_datacollector as fb_datacollector

PICK_DEFAULT_COUNT = 3
PICK_DEFAULT_AUDIENCE_SIZE = 100000
ENG_COPY_STRING = ' - Copy'
CHN_COPY_STRING = ' - 複本'
AI_ADSET_PREFIX = '_AI'
IS_DEBUG = False #debug mode will not modify anything

ACTION_DICT = {
    'bid': AdSet.Field.bid_amount,
    'age': AdSet.Field.targeting, 
    'interest': AdSet.Field.targeting
}

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


# In[ ]:


def make_adset(adset_params):
    account_id = adset_params[AdSet.Field.account_id]
    new_adset = AdSet(parent_id='act_{}'.format(account_id))
    new_adset.update(adset_params)
    new_adset.remote_create(params={'status': 'ACTIVE', })
    return new_adset[AdSet.Field.id]

def retrieve_origin_adset_params(origin_adset_id):
    origin_adset = AdSet(fbid=origin_adset_id)
    origin_adset_params = origin_adset.api_get(fields=FIELDS)
    return origin_adset_params

def get_ad_id_list(adset_id):
    ad_id_list = list()
    adset = AdSet(adset_id)
    ads = adset.get_ads(fields=['id'])
    for ad in ads:
        ad_id_list.append(ad['id'])
    return ad_id_list

def get_account_id_by_adset(adset_id):
    this_adsets = AdSet(adset_id).api_get(fields=["account_id"])
    account_id = this_adsets.get('account_id')
    return account_id

def get_suggestion_target_by_adset(adset_id):
    account_id = get_account_id_by_adset(adset_id)
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    df_target_suggestion = database_fb.retrieve("account_target_suggestion", account_id=account_id, by_request_time=False)[['suggestion_id', 'suggestion_name']]
    database_fb.dispose()
    if not df_target_suggestion.empty:
        target_suggestion_list = df_target_suggestion.to_dict('records')
        pick_index = random.randint(0, len(target_suggestion_list)-1)
        print('[get_suggestion_target_by_adset] pick:', target_suggestion_list[pick_index])
        target_suggestions = target_suggestion_list[pick_index]
        return (target_suggestions['suggestion_id'], target_suggestions['suggestion_name'])
    else:
        return None, None


# In[ ]:


def make_performance_suggest_adset(campaign_id, adsets_active_list): 
    saved_suggest_id_name_dic, saved_suggest_id_size_dic = campaign_suggestion.get_suggestion_not_used(campaign_id)
    print('[make_suggest_adset] saved_suggest_id_name_dic', saved_suggest_id_name_dic, 'saved_suggest_id_size_dic', saved_suggest_id_size_dic)
    for adset_id in adsets_active_list:
        this_adset_interest_id_name_list = campaign_suggestion.retrieve_adset_interest_list(adset_id)
        if this_adset_interest_id_name_list:
            original_adset_id = adset_id
            break
    else:
        print('[make_suggest_adset] no interest adset exist')
        return
    if not saved_suggest_id_name_dic:
        print('[make_suggest_adset] saved_suggest_id_name_dic not exist')
        return
        
    suggest_group_list = []    
    suggest_name_list = []
    # at least 3 suggention and more than 10000 audience size
    pick_count = 0
    pick_total_audience_size = 0
    for suggest_id in saved_suggest_id_name_dic:
        pick_count += 1
        pick_total_audience_size += saved_suggest_id_size_dic.get(suggest_id)
        
        suggest_name = saved_suggest_id_name_dic[suggest_id]
        suggest_name_list.append(suggest_name)
        this_dic = {"id": suggest_id, "name": suggest_name}
        
        suggest_group_list.append(this_dic)
        
        if pick_total_audience_size >= PICK_DEFAULT_AUDIENCE_SIZE and pick_count >= PICK_DEFAULT_COUNT:
            break
    
    print('[make_suggest_adset] suggest_group_list:', suggest_group_list, ' pick_total_audience_size:', pick_total_audience_size)
    
    new_adset_params = retrieve_origin_adset_params(original_adset_id)
    print('[make_suggest_adset] new_adset_params', new_adset_params)
    
    if new_adset_params[AdSet.Field.targeting].get("flexible_spec") == None:
        print('[make_suggest_adset] no flexible_spec')
        return

    suggestion_full_name = '__'.join(suggest_name_list)
    new_adset_params[AdSet.Field.name] = "(BT/{name}/X/AI_{date})BT/{name}/X#[BT,{name}]".format(name=suggestion_full_name,
                                                                                                 date=datetime.date.today().strftime("%Y%m%d"))
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

    print('[make_suggest_adset] new_adset_params after process', new_adset_params)
    new_adset_params[AdSet.Field.id] = None
    new_adset_id = copy_adset_new_target(campaign_id, new_adset_params, original_adset_id)
    return new_adset_id


# In[ ]:


def ad_name_remove_copy_string(ad_id):
    this_ad = facebook_business_ad.Ad(ad_id).api_get(fields=["name"])
    this_ad_name = this_ad.get('name')
    print('[ad_name_remove_copy_string]: before', this_ad_name)
    eng_index, chn_index = this_ad_name.find(ENG_COPY_STRING), this_ad_name.find(CHN_COPY_STRING)
    if eng_index > -1 or chn_index > -1:
        index = list(filter(lambda x: x > -1, [eng_index, chn_index]))[0]
        remove_copy_name = this_ad_name[:index]
        print('[ad_name_remove_copy_string]: after', remove_copy_name)
        try:
            this_ad.api_update(params={'name': remove_copy_name})
        except Exception as e:
            print('[ad name change failed]: ', e)
            pass

def assign_copied_ad_to_new_adset(campaign_id, new_adset_id=None, ad_id=None):
    #need permission first
    account_id = get_account_id_by_adset(new_adset_id)
    my_access_token = permission.get_access_token_by_account(account_id)
    url = permission.FACEBOOK_API_VERSION_URL + str(ad_id) + '/copies'
    
    querystring = {
        "adset_id": "{}".format(new_adset_id),
        "status_option": "INHERITED_FROM_SOURCE"}
    headers = {
        'Authorization': "Bearer {}".format(my_access_token)}
    response = requests.request(
        "POST", url, headers=headers, params=querystring)
    return response.text


def copy_adset_new_target(campaign_id, new_adset_params, original_adset_id):
    new_adset_id = -1
    try:
        new_adset_id = make_adset(new_adset_params)
        print('[copy_adset_new_target] make_adset success, campaign_id:', campaign_id, ' original_adset_id', original_adset_id, ' new_adset_id', new_adset_id)
        time.sleep(10)
        ad_id_list = get_ad_id_list(original_adset_id)
        print('ad_id_list', ad_id_list)
        for ad_id in ad_id_list:
            result_message = assign_copied_ad_to_new_adset(campaign_id, new_adset_id=new_adset_id, ad_id=ad_id)
            print('[copy_adset_new_target] result_message', result_message)
            if 'copied_ad_id' in result_message:
                copied_ad_id = json.loads(result_message).get('copied_ad_id')
                ad_name_remove_copy_string(copied_ad_id)

        return new_adset_id
            
    except Exception as error:
        print('[copy_adset_new_target] this adset is not existed anymore, error:', error)


# In[ ]:


def copy_branding_adset(campaign_id, adset_id, actions, adset_params=None):
    print('[copy_branding_adset] adset_params', adset_params)
    new_adset_params = adset_params
    origin_adset_name = adset_params[AdSet.Field.name]
    original_adset_id = adset_id
    new_adset_params[AdSet.Field.id] = None
    
    # Generate new adset name
    new_adset_name = ''
    index = origin_adset_name.find(AI_ADSET_PREFIX)
    if index > 0:
        new_adset_name = origin_adset_name[:index]  + AI_ADSET_PREFIX
    else:
        new_adset_name = origin_adset_name + AI_ADSET_PREFIX
    
    for i, action in enumerate(actions.keys()):
        if action == 'bid':
            new_adset_params[ACTION_DICT[action]] = math.floor(revert_bid_amount(actions[action]))  # for bid

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
    
    new_adset_id = copy_adset_new_target(campaign_id, new_adset_params, original_adset_id)
    return new_adset_id
            


# In[ ]:


def make_performance_lookalike_adset(campaign_id, adsets_active_list):
    for adset_id in adsets_active_list:
        this_adset_interest_id_name_list = campaign_suggestion.retrieve_adset_interest_list(adset_id)
        if this_adset_interest_id_name_list:
            original_adset_id = adset_id
            break
    else:
        print('[make_suggest_adset] no interest adset exist')
        return
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
    action_name = list(lookalike_audience_dict.keys())[0]
    audience_id = list(lookalike_audience_dict.values())[0]
    targeting["custom_audiences"] = [{"id": audience_id}]
    new_adset_params["name"] = "(LL/{action}_3%/X/AI_{date})LL/{action}/X#[LL,{action}]".format(action=action_name,
                                                                                                date=datetime.date.today().strftime("%Y%m%d"))
    print('==================')
    print('new_adset_params:', new_adset_params)
    
    new_adset_id = copy_adset_new_target(campaign_id, new_adset_params, original_adset_id)
    if new_adset_id:
        lookalike_audience.modify_result_db(campaign_id, lookalike_audience_id, "True")
    return new_adset_id


# In[ ]:


def is_adset_should_close(adset_id, setting_CPA):
    print('[is_adset_should_close] adset_id:', adset_id, ' setting_CPA:', setting_CPA)
    my_adset = fb_datacollector.AdSets(adset_id)
    my_adset_insight_dic = my_adset.get_adset_insights()
#     print(my_adset_insight_dic)
    adset_result_count = my_adset_insight_dic.get('action', 0)
    adset_spending = int(my_adset_insight_dic.get('spend', 0))
    adset_cost_per_result = (adset_spending/adset_result_count) if adset_result_count > 0 else 0
    print('[is_adset_should_close] adset_result_count:', adset_result_count, ' adset_spending:', adset_spending, ' adset_cost_per_result:', adset_cost_per_result)

    if adset_result_count == 0:
        print('[is_adset_should_close] need close , result is 0')
        return True
    elif adset_cost_per_result <= setting_CPA:
        print('[is_adset_should_close] still open')
        return False
    else:
        print('[is_adset_should_close] need close')
        return True


# In[ ]:


#  !jupyter nbconvert --to script facebook_adset_controller.ipynb


# In[ ]:


def fast_test_remove_copy_string(account_id, campaign_id):
    global database_fb
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    permission.init_facebook_api(account_id)
    import facebook_datacollector
    campaign_instance = facebook_datacollector.Campaigns(campaign_id)
    adsets_active_list = campaign_instance.get_adsets_active()
    for original_adset_id in adsets_active_list:
        ad_id_list = get_ad_id_list(original_adset_id)
        [ad_name_remove_copy_string(ad_id) for ad_id in ad_id_list]


# In[ ]:


# global database_fb
# db = database_controller.Database()
# database_fb = database_controller.FB(db)
# campaign_not_opted_list = database_fb.get_performance_campaign().to_dict('records')

# print('df_not_opted len:', len(campaign_not_opted_list))
# print(campaign_not_opted_list)
# for campaign in campaign_not_opted_list:

#     fast_test_remove_copy_string(campaign['account_id'], campaign['campaign_id'])


# In[ ]:


##make suggest adset , test case
# import adgeek_permission as permission
# account_id = 1690972390965768
# campaign_id = 23844021038540408
# original_adset_id = 23844030923290408
# permission.init_facebook_api(account_id)
# make_performance_suggest_adset(campaign_id, [original_adset_id])

