#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import datetime
import math
from copy import deepcopy
import numpy as np
import pandas as pd

from facebook_business.adobjects.adset import AdSet
import facebook_business.adobjects.campaign as facebook_business_campaign

from bid_operator import revert_bid_amount
import database_controller
import facebook_datacollector as collector
import facebook_adapter as adapter
import facebook_currency_handler as fb_currency_handler
import facebook_ai_behavior_log as ai_logger
import facebook_adset_controller as adset_controller
import adgeek_permission as permission


# In[ ]:


IS_DEBUG = False #debug mode will not modify anything

ACTION_BOUNDARY = 0.8

ADSET_MAX_COUNT_CPA = 5
ADSET_MIN_COUNT = 3
ADSET_COPY_COUNT = 3

AI_ADSET_PREFIX = 'AI_'


# In[ ]:


def update_interest(adset_id=None, adset_params=None):
    adset = AdSet(adset_id)
    adset.update(adset_params)
    adset.api_update(
        params={'status': 'PAUSED'}
    )

def update_status(adset_id, status=AdSet.Status.active):
    if IS_DEBUG:
        return
    if status == AdSet.Status.paused:
        ai_logger.save_adset_behavior(adset_id, ai_logger.BehaviorType.CLOSE)
    adset = AdSet(adset_id)
    adset.api_update(
        params={'status': status}
    )

def update_daily_min_spend_target(adset_id):
    if IS_DEBUG:
        return
    adset = AdSet(adset_id)
    adset[AdSet.Field.status] = status
    adset.api_update()

def set_adset_status(campaign_instance, ai_kpi_setting):
    df = database_fb.retrieve('score_7d', campaign_instance.campaign_id)
    adsets_active_list = campaign_instance.get_adsets_active()
    adset_list = []
    adset_action_list = []
    if df.empty:
        print('[get_sorted_adset] Error, no adset score')
        return []
    df_today = df.sort_values(by=['score'], ascending=False)
    print('[get_sorted_adset] df_today\n', df_today)
    adset_list = df_today['adset_id'].unique().tolist()
    for adset in adset_list:
        if str(adset) in adsets_active_list:
            adset_action_list.append(adset)
    adset_for_copy_list, adset_for_off_list = split_adset_list(adset_action_list)
    # current going adset is less than ADSET_MIN_COUNT, not to close any adset
    if len(adsets_active_list) <= ADSET_MIN_COUNT:
        adset_for_off_list = []
    print('[optimize_performance_campaign] adset_list', len(adset_list))
    print('[optimize_performance_campaign] adset_action_list', len(adset_action_list))
    print('[optimize_performance_campaign] adset_for_copy_list', len(adset_for_copy_list))
    print('[optimize_performance_campaign] adset_for_off_list', len(adset_for_off_list))
    close_adset(adset_for_off_list, ai_kpi_setting)

def adset_optimization(campaign_instance):
    adset_list = campaign_instance.get_adsets_active()
    print('    [adset_optimization] adsets_active_list:', adset_list)
    campaign = database_fb.get_one_campaign(campaign_instance.campaign_id).to_dict('records')[0]
    is_target_suggest = campaign.get("is_target_suggest")
    is_lookalike = campaign.get("is_target_suggest")
    if len(adset_list) <= ADSET_MAX_COUNT_CPA:
        if len(adset_list) > 0 and not IS_DEBUG:
            #create one suggestion adset for CPA campaigin
            print('    [adset_optimization] create one suggestion asset for CPA campaigin')
            if is_target_suggest:
                new_adset_id = adset_controller.make_performance_suggest_adset(campaign_instance.campaign_id, adset_list)
                if new_adset_id:
                    ai_logger.save_adset_behavior(new_adset_id, ai_logger.BehaviorType.CREATE)
            #create one lookalike adset for CPA campaigin
            print('    [adset_optimization] create one lookalike asset for CPA campaigin')
            if is_lookalike:
                new_adset_id = adset_controller.make_performance_lookalike_adset(campaign_instance.campaign_id, adset_list)
                if new_adset_id:
                    ai_logger.save_adset_behavior(new_adset_id, ai_logger.BehaviorType.CREATE)

def close_adset(adset_list, ai_kpi_setting):
    for adset_id in adset_list:
        origin_adset_params = adset_controller.retrieve_origin_adset_params(adset_id)
        origin_name = origin_adset_params[AdSet.Field.name]
        if not is_contain_rt_string(origin_name):
            if adset_controller.is_adset_should_close(int(adset_id), ai_kpi_setting):
                update_status(adset_id, status=AdSet.Status.paused)

def adset_sorting_by_score(campaign_instance):
    df = database_fb.retrieve('score_7d', campaign_instance.campaign_id)
    if df.empty:
        print('    [get_sorted_adset] Error, no adset score')
        return []
    else:
        df_today = df.sort_values(by=['score'], ascending=False)
        print('    [get_sorted_adset] df_today', df_today)
        adset_list = df_today['adset_id'].unique().tolist()
        return adset_list

def split_adset_list(adset_list):
    if not adset_list:
        return [], []
    half = math.ceil(len(adset_list) / 2)
    return adset_list[:ADSET_COPY_COUNT], adset_list[half:]

def is_contain_copy_string(adset_name):
    return (AI_ADSET_PREFIX in adset_name)

def is_contain_rt_string(adset_name):
    return ('RT_' in adset_name)

def is_contain_lookalike_string(adset_name):
    return ('LL/' in adset_name)

def modify_opt_result_db(campaign_id, is_optimized):
    #get date
    opt_date = datetime.date.today()
    database_fb.update("campaign_target",
                       {"is_optimized": is_optimized, "optimized_date": opt_date},
                       campaign_id=campaign_id)


# In[ ]:


def get_campaign_name_status(campaign_id):
    this_campaign = facebook_business_campaign.Campaign(campaign_id).api_get(fields=["status", "name"])
    return this_campaign.get('name'), this_campaign.get('status')


# In[ ]:


def optimize_performance_campaign(account_id,
                                  campaign_id,
                                  destination,
                                  destination_max,
                                  charge_type,
                                  destination_type,
                                  custom_conversion_id,
                                  is_optimized,
                                  optimized_date,
                                  cost_per_target,
                                  daily_budget,
                                  daily_charge,
                                  impressions,
                                  ctr,
                                  period,
                                  spend,
                                  ai_spend_cap,
                                  ai_start_date,
                                  ai_stop_date,
                                  ai_status,
                                  spend_cap,
                                  start_time,
                                  stop_time,
                                  target,
                                  desire,
                                  interest,
                                  awareness,
                                  target_left,
                                  target_type,
                                  reach,
                                  is_smart_spending,
                                  is_target_suggest,
                                  is_lookalike,
                                  is_creative_opt,
                                  is_smart_bidding,
                                  actual_metrics,
                                  industry_type,
                                 ):
    print('[optimize_performance_campaign] campaign ', campaign_id)
    is_smart_spending = eval(is_smart_spending)
    is_target_suggest = eval(is_target_suggest)
    is_lookalike = eval(is_lookalike)
    current_flight = (datetime.date.today()-ai_start_date).days + 1
    last_7d_flight_process = 7 / period
    lifetime_flight_process = current_flight / period

    campaign_instance = collector.Campaigns(campaign_id)

    day_dict = campaign_instance.generate_info(date_preset=collector.DatePreset.yesterday)
    last_7d_dict = campaign_instance.generate_info(date_preset=collector.DatePreset.last_7d)
    # this lifetime means ai_start_date and ai_stop_date; 
    lifetime_dict = campaign_instance.generate_info(date_preset=collector.DatePreset.lifetime)
    last_7d_dict['target'] = last_7d_dict.pop('action')
    last_7d_target = int(last_7d_dict['target'])
    lifetime_dict['target'] = lifetime_dict.pop('action')
    lifetime_target = int(lifetime_dict['target'])

    ai_setting_spend_cap = int(ai_spend_cap)
    ai_setting_destination_count = int(destination)
    ai_setting_cost_per_result = ai_setting_spend_cap / ai_setting_destination_count
    print('[optimize_performance_campaign]\n    ai_setting_destination_count:', ai_setting_destination_count,
          '\n    ai_setting_spend_cap:', ai_setting_spend_cap,
          '\n    ai_setting_cost_per_result:', ai_setting_cost_per_result)

#     if lifetime_target > ai_setting_destination_count:
#         modify_opt_result_db(campaign_id, "False")
#         print('[optimize_performance_campaign] lifetime good enough')
#         return

    target = int(day_dict.get('action', 0)) # get by insight

    last_7d_achieving_rate = last_7d_target / (ai_setting_destination_count * last_7d_flight_process)
    lifetime_achieving_rate = lifetime_target / (ai_setting_destination_count * lifetime_flight_process) if (ai_setting_destination_count) != 0 else 0
    print('[achieving rate]', lifetime_achieving_rate, ' current_target', lifetime_target,
          ' \n    destined_target', (ai_setting_destination_count * lifetime_flight_process))
    adsets_active_list = campaign_instance.get_adsets_active()
    
    yesterday_spend = float(day_dict.get('spend', 0))
    if last_7d_achieving_rate < 1:
        print('[optimize_performance_campaign] last_7d status: off standard.\n')
        if lifetime_achieving_rate < ACTION_BOUNDARY:
            # update bid for original existed adset
            print('[optimize_performance_campaign] campaign_daily_budget', daily_budget)
            if not day_dict.get('spend'):
                print('[optimize_performance_campaign] no spend value')            
                return
            if daily_budget and yesterday_spend and (yesterday_spend <= daily_budget * 0.8):
                print('[optimize_performance_campaign] yesterday_spend not enough:', yesterday_spend)            
                if not IS_DEBUG:
                    database_fb.update_init_bid(campaign_id, update_ratio=1.1)
            else:
                print('[optimize_performance_campaign] yesterday_spend is enough, lower bidding')
                if not IS_DEBUG:
                    database_fb.update_init_bid(campaign_id, update_ratio=0.9)
                
                
                
                
        if lifetime_achieving_rate < 1:
            print('[optimize_performance_campaign] lifetime status: off standard.\n')
            # 1.close half (免死)
            adset_list = adset_sorting_by_score(campaign_instance)
            adset_list = [adset for adset in adset_list if str(adset) in adsets_active_list]
            adset_for_copy_list, adset_for_off_list = split_adset_list(adset_list)
            adset_for_off_list = [adset for adset in adset_for_off_list if len(adsets_active_list) >= ADSET_MIN_COUNT]
            close_adset(adset_for_off_list, ai_setting_cost_per_result)
            # 2.add one suggestion
            adset_optimization(campaign_instance)
            print('[optimize_performance_campaign] campaign_daily_budget', daily_budget)
            if not day_dict.get('spend'):
                print('[optimize_performance_campaign] no spend value')            
                return
            if daily_budget and yesterday_spend and (yesterday_spend <= daily_budget * 0.8):
                print('[optimize_performance_campaign] yesterday_spend not enough:', yesterday_spend)            
                if not IS_DEBUG:
                    database_fb.update_init_bid(campaign_id, update_ratio=1.1)
            else:
                print('[optimize_performance_campaign] yesterday_spend is enough, lower bidding')
                if not IS_DEBUG:
                    database_fb.update_init_bid(campaign_id, update_ratio=0.9)
            
            
            
        else:
            print('[optimize_performance_campaign] lifetime status: meet requirements.\n')
            # 1.In [adset for adset in lifetime_adset if adset_cpa <= 1.2 KPI] pick adset who has the lowest cpa.
            lifetime_adsets = campaign_instance.get_adsets()
            adset_insights_list = [
                collector.AdSets(adset_id).get_adset_insights(date_preset=collector.DatePreset.lifetime
                                                             ) for adset_id in lifetime_adsets
            ]
            [adset_insights_list[idx].update({"adset_id": adset_id}) for idx, adset_id in enumerate(lifetime_adsets)]
            df_insights = pd.DataFrame(adset_insights_list)
            df_insights["CPA"] = df_insights.spend / df_insights.action
            df_insights = df_insights[
                (np.isfinite(df_insights.CPA))&(df_insights.CPA < 1.2*ai_setting_cost_per_result)].sort_values(by=['CPA'])
            adset_to_turn_on_list = [adset for adset in list(df_insights.adset_id.unique()) if str(adset) not in adsets_active_list]
            if df_insights.empty:
                # add suggestion
                adset_optimization(campaign_instance)
            elif adset_to_turn_on_list:
                update_status(adset_to_turn_on_list[0], status=AdSet.Status.active)
            # Or, add one suggestion
            # 2. close half (免死)
            adset_list = adset_sorting_by_score(campaign_instance)
            adset_list = [adset for adset in adset_list if str(adset) in adsets_active_list]
            adset_for_copy_list, adset_for_off_list = split_adset_list(adset_list)
            adset_for_off_list = [adset for adset in adset_for_off_list if len(adsets_active_list) >= ADSET_MIN_COUNT]
            close_adset(adset_for_off_list, ai_setting_cost_per_result)
            
            print('[optimize_performance_campaign] campaign_daily_budget', daily_budget)
            if not day_dict.get('spend'):
                print('[optimize_performance_campaign] no spend value')            
                return
            if daily_budget and yesterday_spend and (yesterday_spend <= daily_budget * 0.8):
                print('[optimize_performance_campaign] yesterday_spend not enough:', yesterday_spend)            
                if not IS_DEBUG:
                    database_fb.update_init_bid(campaign_id, update_ratio=1.1)
            else:
                print('[optimize_performance_campaign] yesterday_spend is enough, lower bidding')
                if not IS_DEBUG:
                    database_fb.update_init_bid(campaign_id, update_ratio=0.9)
            
            
            
        modify_opt_result_db(campaign_id, "True")
    else:
        print('[optimize_performance_campaign] last_7d status: meet requirements.\n')
        if lifetime_achieving_rate < 1:
            print('[optimize_performance_campaign] lifetime status: off standard.\n')
            # close one low rank (免死)
            adset_list = adset_sorting_by_score(campaign_instance)
            adset_list = [adset for adset in adset_list if str(adset) in adsets_active_list]
            if adset_list:
                close_adset([adset_list.pop()], ai_setting_cost_per_result)
                modify_opt_result_db(campaign_id, "True")
            else:
                modify_opt_result_db(campaign_id, "False")
                
            print('[optimize_performance_campaign] campaign_daily_budget', daily_budget)
            if not day_dict.get('spend'):
                print('[optimize_performance_campaign] no spend value')            
                return
            if daily_budget and yesterday_spend and (yesterday_spend <= daily_budget * 0.8):
                print('[optimize_performance_campaign] yesterday_spend not enough:', yesterday_spend)            
                if not IS_DEBUG:
                    database_fb.update_init_bid(campaign_id, update_ratio=1.1)
            else:
                print('[optimize_performance_campaign] yesterday_spend is enough')
                
                
                
        else:
            print('[optimize_performance_campaign] lifetime status: meet requirements.\n')
            print('[optimize_performance_campaign] campaign_daily_budget', daily_budget)
            if not day_dict.get('spend'):
                print('[optimize_performance_campaign] no spend value')            
                return
            if daily_budget and yesterday_spend and (yesterday_spend <= daily_budget * 0.8):
                print('[optimize_performance_campaign] yesterday_spend not enough:', yesterday_spend)            
                if not IS_DEBUG:
                    database_fb.update_init_bid(campaign_id, update_ratio=1.1)
            else:
                print('[optimize_performance_campaign] yesterday_spend is enough')
            modify_opt_result_db(campaign_id, "False")


# In[ ]:


def optimize_branding_campaign(account_id,
                               campaign_id,
                               destination,
                               destination_max,
                               charge_type,
                               destination_type,
                               custom_conversion_id,
                               is_optimized,
                               optimized_date,
                               cost_per_target,
                               daily_budget,
                               daily_charge,
                               impressions,
                               ctr,
                               period,
                               spend,
                               ai_spend_cap,
                               ai_start_date,
                               ai_stop_date,
                               ai_status,
                               spend_cap,
                               start_time,
                               stop_time,
                               target,
                               desire,
                               interest,
                               awareness,
                               target_left,
                               target_type,
                               reach,
                               is_smart_spending,
                               is_target_suggest,
                               is_lookalike,
                               is_creative_opt,
                               is_smart_bidding,
                               actual_metrics,
                               industry_type,
                              ):
    print('[optimize_branding_campaign] campaign ', campaign_id)
    # charge_type attribute of first row
    is_smart_spending = eval(is_smart_spending)
    is_target_suggest = eval(is_target_suggest)
    is_lookalike = eval(is_lookalike)
    current_flight = (datetime.date.today()-ai_start_date).days + 1
    last_7d_flight_process = 7 / period
    lifetime_flight_process = current_flight / period

    campaign_daily_budget = daily_budget

    campaign_instance = collector.Campaigns(campaign_id)

    ai_setting_spend_cap = int(ai_spend_cap)
    ai_setting_destination_count = int(destination)
    ai_setting_cost_per_result = ai_setting_spend_cap / ai_setting_destination_count
    print('[optimize_performance_campaign]\n    ai_setting_destination_count:', ai_setting_destination_count,
          '\n    ai_setting_spend_cap:', ai_setting_spend_cap,
          '\n    ai_setting_cost_per_result:', ai_setting_cost_per_result)

    day_dict = campaign_instance.generate_info(date_preset=collector.DatePreset.yesterday)
    lifetime_dict = campaign_instance.generate_info(date_preset=collector.DatePreset.lifetime)
    day_dict['target'] = day_dict.pop('action')
    lifetime_dict['target'] = lifetime_dict.pop('action')
    lifetime_target = int(lifetime_dict['target'])
    if lifetime_target > ai_setting_destination_count:
        print('[optimize_branding_campaign] lifetime good enough')
        modify_opt_result_db(campaign_id, "False")
        return  

    target = 0 # get by insight
    if 'target' in day_dict:
        target = int(day_dict['target'])

    lifetime_achieving_rate = lifetime_target / (ai_setting_destination_count * lifetime_flight_process)
    print('[achieving rate]', lifetime_achieving_rate, ' current_target', lifetime_target,
          ' \n    destined_target', (ai_setting_destination_count * lifetime_flight_process))
    
    if lifetime_achieving_rate > ACTION_BOUNDARY and lifetime_achieving_rate < 1:
        print('[optimize_branding_campaign] 0.8 < lifetime_achieving_rate < 1')
    elif lifetime_achieving_rate < ACTION_BOUNDARY:
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
        modify_opt_result_db(campaign_id, "False")
        return

    # current going adset is less than ADSET_MIN_COUNT, not to close any adset
    adsets_active_list = campaign_instance.get_adsets_active()
    print('[optimize_branding_campaign] adsets_active_list:', adsets_active_list)

    set_adset_status(campaign_instance, ai_setting_cost_per_result)

    # get ready to duplicate
    actions = {'bid': None, 'age': list(), 'interest': None}
    actions_list = list()

    #get adset bid for this campaign
    fb_adapter = adapter.FacebookCampaignAdapter(campaign_id, database_fb)
    fb_adapter.retrieve_campaign_attribute()

    adset_list = campaign_instance.get_adsets_active()
    adset_for_copy_list, adset_for_off_list = split_adset_list(adset_list)
    
    for adset_id in adset_for_copy_list:
        # bid adjust
        bid = fb_adapter.init_bid_dict.get(int(adset_id))
        #error handle: the adset did not have score
        if bid is None:
            print('[optimize_branding_campaign] adset bid is None')
            continue
        bid = fb_currency_handler.get_proper_bid(campaign_id, bid)

        actions.update({'bid': bid})
        origin_adset_params = adset_controller.retrieve_origin_adset_params(adset_id)
        origin_adset_params[AdSet.Field.id] = None
        origin_name = origin_adset_params[AdSet.Field.name]
        
        if is_contain_rt_string(origin_name) or is_contain_lookalike_string(origin_name):
            continue
        
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
                current_adset_max = current_adset_min + age_interval if (current_adset_min + age_interval) < adset_max else adset_max
                actions['age'][0] = str(current_adset_min) + '-' + str(current_adset_max)
                adset_min = current_adset_max + 1
                actions_copy = deepcopy(actions)
                copy_result_new_adset_id = adset_controller.copy_branding_adset(campaign_id, 
                                                                                adset_id, 
                                                                                actions_copy, 
                                                                                origin_adset_params)
                if copy_result_new_adset_id:
                    ai_logger.save_adset_behavior(copy_result_new_adset_id, ai_logger.BehaviorType.COPY)
    modify_opt_result_db(campaign_id, "True")


# In[ ]:


def optimize_campaign(campaign_id):
    print('[optimize_campaign] campaign_id', campaign_id)
    campaign = database_fb.get_one_campaign(campaign_id).to_dict('records')[0]
    permission.init_facebook_api(campaign['account_id'])
    print(campaign)
    campaign_name, campaign_fb_status = get_campaign_name_status(campaign_id)
    print(campaign_id, campaign_fb_status, campaign_name)
    if campaign_fb_status == 'ACTIVE':
        print('[optimize_campaign] destination_type', campaign['destination_type'])
        if campaign['destination_type'] in collector.PERFORMANCE_CAMPAIGN_LIST:
            optimize_performance_campaign(**campaign)
        elif campaign['destination_type'] in collector.BRANDING_CAMPAIGN_LIST:
            optimize_branding_campaign(**campaign)
        elif campaign['destination_type'] in collector.CUSTOM_CAMPAIGN_LIST:
            optimize_performance_campaign(**campaign)
        else:
            print('[optimize_campaign] error, not optimize')


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


# In[ ]:


#nate test
# adset_controller.make_suggest_adset(23843604240180098,23843467729120098)


# In[ ]:


# !jupyter nbconvert --to script facebook_externals.ipynb


# In[ ]:


#nate test
#copy adset for branding adset
# account_id = 350498128813378
# campaign_id = 23844199663310559
# adset_id = 23844199663710559
# adset_min = 20
# adset_max = 30
# permission.init_facebook_api(account_id)
# actions_copy = { 'age': list(), 'interest': None}
# actions_copy['age'].append( str(adset_min) + '-' + str(adset_max))
# origin_adset_params = adset_controller.retrieve_origin_adset_params(adset_id)
# adset_controller.copy_branding_adset(campaign_id, adset_id, actions_copy, origin_adset_params)

