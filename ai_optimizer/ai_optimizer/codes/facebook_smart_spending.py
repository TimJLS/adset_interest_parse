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
import mysql_adactivity_save as mysql_saver
import facebook_currency_handler as currency_handler
import datetime

IS_DEBUG = True
DESTINATION_SPEED_RATIO_VALUE = 1.1


# In[2]:


def update_campaign_daily_budget(campaign_id, daily_budget):
    if IS_DEBUG:
        return
    this_campaign = facebook_business_campaign.Campaign( campaign_id)
    this_campaign.update({
        facebook_business_campaign.Campaign.Field.daily_budget: daily_budget
    })
    try:
        this_campaign.remote_update()
    except Exception as error:
        print('[update_campaign_daily_budget] error:', error)


def update_campaign_bidding(campaign_id, bid_up_ratio):
    if IS_DEBUG:
        return
    mysql_saver.adjust_init_bid(campaign_id, bid_up_ratio)
    
def get_campaign_name_status(campaign_id):
    this_campaign = facebook_business_campaign.Campaign( campaign_id).remote_read(fields=["status", "name"])
    return this_campaign.get('name'), this_campaign.get('status')
    
    


# In[3]:


def get_campaign_status(campaign_id):
    currency = currency_handler.get_currency_by_campaign(campaign_id)
        
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = 'SELECT destination, destination_max, ai_spend_cap, target, target_left, spend , ai_start_date, ai_stop_date FROM campaign_target where campaign_id = {}'.format(campaign_id)
    my_cursor.execute(sql)
    destination, destination_max, ai_spend_cap, current_target_count, left_target_count, current_total_spend, ai_start_date, ai_stop_date  = my_cursor.fetchone()
    my_db.commit()
    my_db.close()
    
    if currency == 'USD':
        ai_spend_cap = ai_spend_cap / 100
    
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
    print('[get_campaign_status] campaign_id', campaign_id)
    print('[get_campaign_status] kpi_cpc', kpi_cpc)
    print('[get_campaign_status] current_cpc', current_cpc)    
    print('[get_campaign_status] destination', destination)
    print('[get_campaign_status] destination_max', destination_max)    
    print('[get_campaign_status] current_target_count', current_target_count)
    print('[get_campaign_status] left_target_count', left_target_count)
    print('[get_campaign_status] --')        
    print('[get_campaign_status] currency', currency)    
    print('[get_campaign_status] ai_spend_cap', ai_spend_cap)
    print('[get_campaign_status] current_total_spend', current_total_spend)    
    print('[get_campaign_status] left_money_can_spend', left_money_can_spend) 
    print('[get_campaign_status] left_money_can_spend_per_day', left_money_can_spend_per_day) 
    print('[get_campaign_status] ai_daily_budget', ai_daily_budget)
    print('[get_campaign_status] --')    
    print('[get_campaign_status] ai_start_date', ai_start_date)    
    print('[get_campaign_status] ai_stop_date', ai_stop_date)   
    print('[get_campaign_status] ai_period', ai_period) 
    print('[get_campaign_status] ai_left_days', ai_left_days)   
    print('[get_campaign_status] ai_running_days', ai_running_days)   
    print('[get_campaign_status] --')    
    print('[get_campaign_status] max_cpc_for_future', max_cpc_for_future)
    print('[get_campaign_status] max_percent_arise_for_future', max_percent_arise_for_future)
    print('[get_campaign_status] destination_count_until_today', destination_count_until_today) 
    print('[get_campaign_status] destination_speed_ratio', destination_speed_ratio) 
    print('[get_campaign_status] --')
    
    if left_money_can_spend < 0:
        print('[get_campaign_status] Error, spend too much money!!!')  
    elif current_target_count >= destination:
        if destination_max is None:
            print('[get_campaign_status][spend money] destination is already satisfied, up the bid to spend money')
            bid_up_ratio = 1.1

            if currency == 'USD':
                left_money_can_spend_per_day = int(left_money_can_spend_per_day * 100)    

            print('[get_campaign_status][spend money] action-> update_campaign_daily_budget', left_money_can_spend_per_day)     
            print('[get_campaign_status][spend money] action-> bid_up_ratio', bid_up_ratio)  
            
            update_campaign_daily_budget(campaign_id, int(left_money_can_spend_per_day))
            update_campaign_bidding(campaign_id, bid_up_ratio)             
        else:
            print('[get_campaign_status][save money] destination is already satisfied, destination:', destination , ' destination_max:' ,destination_max)
            if current_target_count > destination_max:
                print('[get_campaign_status][save money] set daily budget as 10')
                update_campaign_daily_budget(campaign_id, 10)

            
    else:
        print('[get_campaign_status] destination not satisfied')
        
        if ai_running_days >= ai_left_days: #over half period
            print('[get_campaign_status] over half period')
            
            if destination_max is None:
                print('[get_campaign_status] need to spend all money')
                bid_up_ratio = max_percent_arise_for_future * ( ai_running_days/ai_period )
                bid_price = kpi_cpc * bid_up_ratio

                if currency == 'USD':
                    left_money_can_spend_per_day = int(left_money_can_spend_per_day * 100)
                    bid_price = int(bid_price * 100)

                print('[get_campaign_status][spend money] action-> update_campaign_daily_budget', left_money_can_spend_per_day)     
                update_campaign_daily_budget(campaign_id, int(left_money_can_spend_per_day))

                if destination_speed_ratio >= DESTINATION_SPEED_RATIO_VALUE: # speed good, can up bid to use money
                    print('[get_campaign_status][spend money] speed good, can spend all money')
                    
                    print('[get_campaign_status][spend money] action-> bid_up_ratio', bid_up_ratio)  
                    print('[get_campaign_status][spend money] action-> bid_price', bid_price)   
                    update_campaign_bidding(campaign_id, bid_price)
                    
                else:
                    print('[get_campaign_status][spend money] destination_max is None, destination_speed_ratio too low')
            else:
                if destination_speed_ratio >= 1: 
                    print('[get_campaign_status][save money] speed good, destination:', destination , ' destination_max:' ,destination_max, 'current_target_count:', current_target_count)
                    max_achieve_count = destination_max - current_target_count
                    max_achieve_count_per_day = max_achieve_count / ai_left_days
                    print('[get_campaign_status][save money] max_achieve_count', max_achieve_count) 
                    print('[get_campaign_status][save money] max_achieve_count_per_day', max_achieve_count_per_day)      

                    campaign_daily_budget_revised = math.ceil(max_achieve_count_per_day) * kpi_cpc
                    if currency == 'USD':
                        campaign_daily_budget_revised = int(campaign_daily_budget_revised * 100)

                    print('[get_campaign_status][save money] action-> update_campaign_daily_budget campaign_daily_budget_revised', campaign_daily_budget_revised)     
                    update_campaign_daily_budget(campaign_id, int(campaign_daily_budget_revised))
                    
                else:
                    print('[get_campaign_status][save money] destination_max exist, destination_speed_ratio too low')
                    

        else:
            print('[get_campaign_status] less than half period, do nothing')
    print('[get_campaign_status] finish---------------------------------------------------')
    
    
    

    
    
    


# In[4]:


def main():
#     get_campaign_status(23843605741390744)

    df_branding = mysql_saver.get_running_branding_campaign()
    for index, row in df_branding.iterrows():
        campaign_id = row['campaign_id']
        charge_type = row['charge_type']
        campaign_name , campaign_fb_status = get_campaign_name_status(campaign_id)
        print('[main] campaign_id', campaign_id, charge_type, campaign_fb_status, campaign_name)
        
        if campaign_fb_status == 'ACTIVE':
            get_campaign_status(campaign_id)
            print()
            print()            
        
    
    print('-------',datetime.datetime.now().date(), '-------finish-------')
    

    


# In[5]:




if __name__ == "__main__":
#     main()
    get_campaign_status(23843537403310559)


# In[6]:


# UPDATE `campaign_target` SET  `destination`=100,`destination_max`=110 WHERE `campaign_id` = 23843605741390744

