#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
import datetime
import time
import math
import json

from facebook_business.api import FacebookAdsApi
# from facebook_business.adobjects.adaccount import AdAccount
import facebook_business.adobjects.adset as facebook_business_adset
# from facebook_business.adobjects.ad import Ad
import facebook_business.adobjects.campaign as facebook_business_campaign
# from facebook_business.adobjects.adcreative import AdCreative
# from facebook_business.adobjects.adactivity import AdActivity
# from facebook_business.adobjects.insightsresult import InsightsResult
import facebook_business.adobjects.adsinsights as facebook_business_adsinsights

import facebook_datacollector as fb_collector
import database_controller
import facebook_currency_handler as currency_handler
import adgeek_permission as permission
import datetime

IS_DEBUG = False
DESTINATION_SPEED_RATIO_VALUE = 1.1


# In[2]:


def update_campaign_daily_budget(campaign_id, daily_budget):
    daily_budget = int(daily_budget)
    print('[update_campaign_daily_budget] daily_budget:', daily_budget)
    
    if IS_DEBUG:
        return
            
    this_campaign = facebook_business_campaign.Campaign(campaign_id)
    this_campaign.update({
        facebook_business_campaign.Campaign.Field.daily_budget: daily_budget
    })
    
    try:
        this_campaign.remote_update()
    except Exception as error:
        print('[update_campaign_daily_budget] error:', error)


def update_campaign_bidding_ratio(campaign_id, bid_up_ratio):
    print('[update_campaign_bidding_ratio] bid_up_ratio:', bid_up_ratio)
    if IS_DEBUG:
        return
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    database_fb.update_init_bid(campaign_id, bid_up_ratio)
    
def get_campaign_name_status(campaign_id):
    this_campaign = facebook_business_campaign.Campaign( campaign_id).remote_read(fields=["status", "name"])
    return this_campaign.get('name'), this_campaign.get('status')
    
def get_campaign_daily_budget(campaign_id):
    this_campaign = facebook_business_campaign.Campaign( campaign_id).remote_read(fields=["daily_budget"])
    daily_budget = None
    try:
        daily_budget = int(this_campaign.get('daily_budget'))
    except:
        print('[get_campaign_daily_budget] no daily budget')
    return daily_budget

def set_campaign_daily_budget_lower(campaign_id, ai_daily_budget, lower_rate):
    #facebook can not set daily budget too low , so that we use currect daily budget * lower_rate
    print('[smart_spending_branding][save money] action-> update_campaign_daily_budget, lower_rate:', lower_rate)     
    current_daily_budget = get_campaign_daily_budget(campaign_id) 
    if current_daily_budget:
        update_campaign_daily_budget(campaign_id, current_daily_budget * lower_rate)
    else:
        update_campaign_daily_budget(campaign_id, ai_daily_budget * lower_rate)


# In[3]:


def smart_spending_branding(campaign_id):
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    df_list = database_fb.retrieve("campaign_target", campaign_id=campaign_id, by_request_time=False).to_dict('records')
    
    campaign_target_dict = df_list[0]
    destination = campaign_target_dict.get('destination')
    destination_max = campaign_target_dict.get('destination_max')
    ai_spend_cap = campaign_target_dict.get('ai_spend_cap')
    current_target_count = campaign_target_dict.get('target')
    left_target_count = campaign_target_dict.get('target_left')
    current_total_spend = campaign_target_dict.get('spend')
    ai_start_date = campaign_target_dict.get('ai_start_date')
    ai_stop_date = campaign_target_dict.get('ai_stop_date')
    
    currency = currency_handler.get_currency_by_campaign(campaign_id)
    #avoid error
    if current_target_count is None or current_target_count == 0:
        current_target_count = 1
    
    ai_period = (ai_stop_date - ai_start_date ).days + 1
    today = datetime.date.today()
    ai_left_days = (ai_stop_date - today ).days + 1
    ai_running_days = (today - ai_start_date ).days + 1
    
    ai_daily_budget = ai_spend_cap / ai_period
    left_money_can_spend = ai_spend_cap - current_total_spend
    left_money_can_spend_per_day = left_money_can_spend / ai_left_days
    max_cpc_for_future = left_money_can_spend / left_target_count if left_target_count>0 else left_money_can_spend
    kpi_cpc = ai_spend_cap / destination
    current_cpc =  current_total_spend / current_target_count
    max_percent_arise_for_future = max_cpc_for_future / kpi_cpc
    
    destination_count_until_today = destination * (ai_running_days / ai_period)
    destination_speed_ratio = current_target_count / destination_count_until_today
            
    print('[smart_spending_branding] campaign_id', campaign_id)
    print('[smart_spending_branding] kpi_cpc', kpi_cpc)
    print('[smart_spending_branding] current_cpc', current_cpc)    
    print('[smart_spending_branding] destination', destination)
    print('[smart_spending_branding] destination_max', destination_max)    
    print('[smart_spending_branding] current_target_count', current_target_count)
    print('[smart_spending_branding] left_target_count', left_target_count)
    print('[smart_spending_branding] --')        
    print('[smart_spending_branding] currency', currency)    
    print('[smart_spending_branding] ai_spend_cap', ai_spend_cap)
    print('[smart_spending_branding] current_total_spend', current_total_spend)    
    print('[smart_spending_branding] left_money_can_spend', left_money_can_spend) 
    print('[smart_spending_branding] left_money_can_spend_per_day', left_money_can_spend_per_day) 
    print('[smart_spending_branding] ai_daily_budget', ai_daily_budget)
    print('[smart_spending_branding] --')    
    print('[smart_spending_branding] ai_start_date', ai_start_date)    
    print('[smart_spending_branding] ai_stop_date', ai_stop_date)   
    print('[smart_spending_branding] ai_period', ai_period) 
    print('[smart_spending_branding] ai_left_days', ai_left_days)   
    print('[smart_spending_branding] ai_running_days', ai_running_days)   
    print('[smart_spending_branding] --')    
    print('[smart_spending_branding] max_cpc_for_future', max_cpc_for_future)
    print('[smart_spending_branding] max_percent_arise_for_future', max_percent_arise_for_future)
    print('[smart_spending_branding] destination_count_until_today', destination_count_until_today) 
    print('[smart_spending_branding] destination_speed_ratio', destination_speed_ratio) 
    print('[smart_spending_branding] --')
    
    
    #need to update daily budget everyday
    if left_money_can_spend_per_day > 0:
        update_campaign_daily_budget(campaign_id, left_money_can_spend_per_day)
    
    if left_money_can_spend < 0:
        print('[smart_spending_branding] Error, spend too much money!!!')  
    elif current_target_count >= destination:
        if destination_max is None:
            print('[smart_spending_branding][spend money] destination is already satisfied, up the bid to spend money')
            bid_up_ratio = 1.1
            update_campaign_bidding_ratio(campaign_id, bid_up_ratio)             
        else:
            print('[smart_spending_branding][save money] destination is already satisfied, destination:', destination , ' destination_max:' ,destination_max)
            if current_target_count > destination_max:
                print('[smart_spending_branding][save money] set daily budget multiply 0.5')
                set_campaign_daily_budget_lower(campaign_id, ai_daily_budget, 0.5)
            else: 
#                 max_achieve_count = destination_max - current_target_count
#                 max_achieve_count_per_day = max_achieve_count / ai_left_days
#                 print('[smart_spending_branding][save money] destination count satifisted, max_achieve_count', max_achieve_count) 
#                 print('[smart_spending_branding][save money] destination count satifisted, max_achieve_count_per_day', max_achieve_count_per_day)      

#                 campaign_daily_budget_revised = math.ceil(max_achieve_count_per_day) * current_cpc

                #facebook can not set daily budget too low , so that we use currect daily budget * 0.75
                print('[smart_spending_branding][save money] action-> update_campaign_daily_budget')     
                set_campaign_daily_budget_lower(campaign_id, ai_daily_budget, 0.75)

    else:
        print('[smart_spending_branding] destination not satisfied')
        
        if ai_running_days >= ai_left_days: #over half period
            print('[smart_spending_branding] over half period')
            
            if destination_max is None:
                print('[smart_spending_branding] need to spend all money')
                
                if destination_speed_ratio >= DESTINATION_SPEED_RATIO_VALUE: # speed good, can up bid to use money
                    print('[smart_spending_branding][spend money] speed good, can spend all money')
                    bid_up_ratio = 1.1
                    update_campaign_bidding_ratio(campaign_id, bid_up_ratio)
                else:
                    print('[smart_spending_branding][spend money] destination_max is None, destination_speed_ratio too low')
            else:
                if destination_speed_ratio >= 1: 
                    print('[smart_spending_branding][save money] speed good, destination:', destination , ' destination_max:' ,destination_max, 'current_target_count:', current_target_count)
#                     max_achieve_count = destination_max - current_target_count
#                     max_achieve_count_per_day = max_achieve_count / ai_left_days
#                     print('[smart_spending_branding][save money] max_achieve_count', max_achieve_count) 
#                     print('[smart_spending_branding][save money] max_achieve_count_per_day', max_achieve_count_per_day)      

#                     campaign_daily_budget_revised = math.ceil(max_achieve_count_per_day) * kpi_cpc

#                     print('[smart_spending_branding][save money] action-> update_campaign_daily_budget campaign_daily_budget_revised', campaign_daily_budget_revised)     
#                     update_campaign_daily_budget(campaign_id, int(campaign_daily_budget_revised))
                    #facebook can not set daily budget too low , so that we use currect daily budget * 0.75
                    print('[smart_spending_branding][save money] action-> update_campaign_daily_budget')     
                    set_campaign_daily_budget_lower(campaign_id, 0.75)
                    
                else:
                    print('[smart_spending_branding][save money] destination_max exist, destination_speed_ratio too low')
                    

        else:
            print('[smart_spending_branding] less than half period, do nothing')
    print('[smart_spending_branding] finish---------------------------------------------------')
    


# In[4]:


def smart_spending_performance(campaign_id):
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    df_list = database_fb.retrieve("campaign_target", campaign_id=campaign_id, by_request_time=False).to_dict('records')

    campaign_target_dict = df_list[0]
    destination = campaign_target_dict.get('destination')
    destination_max = campaign_target_dict.get('destination_max')
    ai_spend_cap = campaign_target_dict.get('ai_spend_cap')
    current_target_count = campaign_target_dict.get('target')
    left_target_count = campaign_target_dict.get('target_left')
    current_total_spend = campaign_target_dict.get('spend')
    ai_start_date = campaign_target_dict.get('ai_start_date')
    ai_stop_date = campaign_target_dict.get('ai_stop_date')
    
    currency = currency_handler.get_currency_by_campaign(campaign_id)
    #avoid error
    if current_target_count is None or current_target_count == 0:
        current_target_count = 1
    
    ai_period = (ai_stop_date - ai_start_date ).days + 1
    today = datetime.date.today()
    ai_left_days = (ai_stop_date - today ).days + 1
    ai_running_days = (today - ai_start_date ).days + 1
    
    ai_daily_budget = ai_spend_cap / ai_period
    left_money_can_spend = ai_spend_cap - current_total_spend
    left_money_can_spend_per_day = left_money_can_spend / ai_left_days
    max_cpc_for_future = left_money_can_spend / left_target_count if left_target_count>0 else left_money_can_spend
    kpi_cpc = ai_spend_cap / destination
    current_cpc =  current_total_spend / current_target_count
    max_percent_arise_for_future = max_cpc_for_future / kpi_cpc
    
    destination_count_until_today = destination * (ai_running_days / ai_period)
    destination_speed_ratio = current_target_count / destination_count_until_today
    print('[smart_spending_performance] campaign_id', campaign_id)
    print('[smart_spending_performance] kpi_cpc', kpi_cpc)
    print('[smart_spending_performance] current_cpc', current_cpc)    
    print('[smart_spending_performance] destination', destination)
    print('[smart_spending_performance] destination_max', destination_max)    
    print('[smart_spending_performance] current_target_count', current_target_count)
    print('[smart_spending_performance] left_target_count', left_target_count)
    print('[smart_spending_performance] --')        
    print('[smart_spending_performance] currency', currency)    
    print('[smart_spending_performance] ai_spend_cap', ai_spend_cap)
    print('[smart_spending_performance] current_total_spend', current_total_spend)    
    print('[smart_spending_performance] left_money_can_spend', left_money_can_spend) 
    print('[smart_spending_performance] left_money_can_spend_per_day', left_money_can_spend_per_day) 
    print('[smart_spending_performance] ai_daily_budget', ai_daily_budget)
    print('[smart_spending_performance] --')    
    print('[smart_spending_performance] ai_start_date', ai_start_date)    
    print('[smart_spending_performance] ai_stop_date', ai_stop_date)   
    print('[smart_spending_performance] ai_period', ai_period) 
    print('[smart_spending_performance] ai_left_days', ai_left_days)   
    print('[smart_spending_performance] ai_running_days', ai_running_days)   
    print('[smart_spending_performance] --')    
    print('[smart_spending_performance] max_cpc_for_future', max_cpc_for_future)
    print('[smart_spending_performance] max_percent_arise_for_future', max_percent_arise_for_future)
    print('[smart_spending_performance] destination_count_until_today', destination_count_until_today) 
    print('[smart_spending_performance] destination_speed_ratio', destination_speed_ratio) 
    print('[smart_spending_performance] --')
    
    #need to update daily budget everyday
    if left_money_can_spend_per_day > 0:
        update_campaign_daily_budget(campaign_id, int(left_money_can_spend_per_day))
    
    if left_money_can_spend < 0:
        print('[smart_spending_performance] Error, spend too much money!!!')  
    else:
        if (current_target_count >= destination) or (ai_running_days >= ai_left_days and destination_speed_ratio >= 1):
            spend_until_today = round(ai_spend_cap * (ai_running_days / ai_period) , 2)
            if current_total_spend < spend_until_today:
                print('[smart_spending_performance] more than target count, spend not enough, up bidding to spend money')  
                bid_up_ratio = 1.1
                update_campaign_bidding_ratio(campaign_id, bid_up_ratio)
        


# In[5]:


def process_branding_campaign():
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    campaign_list = database_fb.get_branding_campaign().to_dict('records')
    for campaign in campaign_list:
        account_id = campaign.get("account_id")
        campaign_id = campaign.get("campaign_id")
        charge_type = campaign.get("charge_type")
        permission.init_facebook_api(account_id)
        campaign_name , campaign_fb_status = get_campaign_name_status(campaign_id)
        print('[process_branding_campaign] campaign_id', campaign_id, charge_type, campaign_fb_status, campaign_name)
        
#         if campaign_id != 23843628364880022:
#             continue

        if not bool(campaign.get('is_smart_spending')):
            continue
        if campaign_fb_status == 'ACTIVE':
            smart_spending_branding(campaign_id)
    
    print('-------',datetime.datetime.now().date(), '-------all finish-------')
    

    


# In[6]:


def process_performance_campaign():
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    campaign_list = database_fb.get_performance_campaign().to_dict('records')
    for campaign in campaign_list:
        account_id = campaign.get("account_id")
        campaign_id = campaign.get("campaign_id")
        charge_type = campaign.get("charge_type")
        permission.init_facebook_api(account_id)
        campaign_name , campaign_fb_status = get_campaign_name_status(campaign_id)
        print('[process_performance_campaign] campaign_id', campaign_id, charge_type, campaign_fb_status, campaign_name)
        
#         if campaign_id != 23843569311660559:
#             continue
        if not bool(campaign.get('is_smart_spending')):
            continue
        if campaign_fb_status == 'ACTIVE':
            smart_spending_performance(campaign_id)
    
    print('-------',datetime.datetime.now().date(), '-------all finish-------')


# In[7]:




if __name__ == "__main__":
    process_branding_campaign()
    process_performance_campaign()


# In[8]:


# UPDATE `campaign_target` SET  `destination`=100,`destination_max`=110 WHERE `campaign_id` = 23843605741390744


# In[9]:




# def is_campaign_adjust_dayily_budget(campaign_id):
#     this_campaign = facebook_business_campaign.Campaign( campaign_id).remote_read(fields=["spend_cap"])
#     return this_campaign.get('spend_cap')

# permission.init_facebook_api(350498128813378)
# result = is_campaign_adjust_dayily_budget(23843537403310559)
# print(result)


# In[10]:


# smart_spending_performance(23842880697850266)


# In[11]:


# !jupyter nbconvert --to script facebook_smart_spending.ipynb


# In[ ]:




