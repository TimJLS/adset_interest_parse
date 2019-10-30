#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import uuid
from googleads import adwords
import sys
import pandas as pd
import numpy as np
import math
import datetime
from copy import deepcopy
import database_controller
import gdn_datacollector as collector
import adgeek_permission as permission
import google_adwords_controller as controller
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
DEVICE_BALANCE_PROPORTION = 0.5
TIME_RANGE = 3
MINIMUM_SPEND = 10
DAY_HOUR = 24
DEVICE_CRITERION = {
    'Desktop': 30000,
    'HighEndMobile': 30001,
    'ConnectedTv': 30004,
    'Tablet': 30002,
}


# In[ ]:


def assign_bid_modifier(adwords_client, ad_group_id, **bid_modifier_dict,):
    
    for device in bid_modifier_dict:
        operations = []
        operand = { 'adGroupId': ad_group_id  }
        criterion = {'xsi_type':'Platform'}
        
        device_id = DEVICE_CRITERION[device]
        bid_modifier_ratio = bid_modifier_dict[device]
        
        criterion['id'] = device_id
        operand['criterion'] = criterion
        operand['bidModifier'] = bid_modifier_ratio
        
        operations.append(
            {
                'operator': 'ADD',
                'operand': operand
            })
        
        ad_group_bid_modifier_service = adwords_client.GetService( 'AdGroupBidModifierService', version='v201809')
        resp = ad_group_bid_modifier_service.mutate(operations)
#         print('[resp]: ', resp)
    return resp


# In[ ]:


def retrieve_bid_modifier(client, ad_group_id):
    ad_group_bid_modifier_service = adwords_client.GetService(
        'AdGroupBidModifierService', version='v201809')
    # Get all ad group bid modifiers for the campaign.
    selector = {
        'fields': ['CampaignId', 'AdGroupId', 'BidModifier', 'Id', 'PlatformName'],
        'predicates': [
            {
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values': [ad_group_id]
            }
        ]
    }
    resp = ad_group_bid_modifier_service.get(selector)
    return resp['entries']


# In[ ]:


def bid_modifier_adjust(controller_ad_group, device_target, direction):
    resps = controller_ad_group.bid_modifier.retrieve()
    bid_modifier_dict = dict()
    # Retrieve bid modifier
    for resp in resps:
        platform = resp['criterion']['platformName']
        bid_modifier = resp['bidModifier']
        if bid_modifier == -1 or bid_modifier is None:
            bid_modifier = 1
        bid_modifier_dict.update({platform: bid_modifier})
    
    # Adjust bid modifier
    for device in bid_modifier_dict.keys():
        if device == device_target:
            if direction == 'up':
                if bid_modifier_dict[device] <=2:
                    bid_modifier_dict[device] += 0.1 
            elif direction == 'down':
                if bid_modifier_dict[device] >= 0.1:
                    bid_modifier_dict[device] -= 0.1 
       
    # Update back
    print('[ad_group_id]: ', controller_ad_group.ad_group_id)
    print('[bid_modifier_dict]: ', bid_modifier_dict)
    resp = controller_ad_group.bid_modifier.update(bid_modifier_dict)
    return resp


# In[ ]:


def get_campaign_budget(adwords_client, campaign_id):
    adword_service = adwords_client.GetService('CampaignService', version='v201809')
    selector = [{
        'fields': 'Amount',
        'predicates': [{
            'field': 'CampaignId',
            'operator': 'EQUALS',
            'values': campaign_id
        }]
    }]

    ad_params = adword_service.get(selector)
    if 'entries' in ad_params:
#         print('ad_params', ad_params)
        for ad_dic in ad_params['entries']:
            if 'budget' in ad_dic and 'amount' in ad_dic['budget'] and 'microAmount' in ad_dic['budget']['amount']:
                microAmount = ad_dic['budget']['amount']['microAmount']
                amount = microAmount/ 1000000
                return amount


# In[ ]:


def main():
    db = database_controller.Database
    database_gdn = database_controller.GDN( db )
    performance_campaign_list = database_gdn.get_performance_campaign().to_dict('records')
    performance_campaign_list = [ campaign for campaign in performance_campaign_list if eval(campaign['is_device_pro_rata']) ]
    print(performance_campaign_list)
    for campaign in performance_campaign_list:
        customer_id = campaign.get('customer_id')
        campaign_id = campaign.get('campaign_id')
        adwords_client = permission.init_google_api(customer_id)
        print('[current time]: ', datetime.datetime.now())
        print('[campaign_id]: ', campaign_id)
        # initiate services
        campaign_service_container = controller.CampaignServiceContainer( customer_id )
        ad_group_service_container = controller.AdGroupServiceContainer( customer_id )
        collector_campaign = collector.Campaign(customer_id, campaign_id)
        controller_campaign = controller.Campaign(campaign_service_container, campaign_id)
        daily_budget = controller_campaign.get_budget()
        if not daily_budget:
            print('camp.ai_spend_cap:', collector_campaign.ai_spend_cap, 'ai_period:', collector_campaign.ai_period)
            daily_budget =  collector_campaign.ai_spend_cap / int(collector_campaign.ai_period)
        controller_campaign = controller.Campaign(ad_group_service_container, campaign_id)
        ad_groups = controller_campaign.get_ad_groups()
        daily_budget_per_group = daily_budget / len(ad_groups)
        print('[main] ad_group_id_list count:', len(ad_groups))
        print('[main] daily_budget_per_group:', daily_budget_per_group)
        for controller_ad_group in ad_groups:
            print('[ad_group_id]: ', controller_ad_group.ad_group_id)
            collector_ad_group = collector.AdGroup( customer_id, campaign_id, controller_ad_group.ad_group_id )
            # Retrieve hourly seperated report
            hourly_insights = collector_ad_group.get_adgroup_insights(date_preset='TODAY', by_hour=True)
            df_hourly_insights = pd.DataFrame(hourly_insights).sort_values(by=['hour_of_day']).reset_index(drop=True)  
            # Check last time interval spend is normal or not
            current_hour = datetime.datetime.now().hour
            last_interval_spend = df_hourly_insights[ df_hourly_insights.hour_of_day.between(current_hour-TIME_RANGE, current_hour, inclusive=True)]['spend'].sum()
            adgroup_today_spend = df_hourly_insights[ df_hourly_insights.hour_of_day.between(0, current_hour, inclusive=True)]['spend'].sum()
            adgroup_now_should_spend = daily_budget_per_group * (current_hour / DAY_HOUR)
            
            #handle money spend spend too slow 
            print('[main] current_hour:', current_hour, ' last_interval_spend:', last_interval_spend)
            print('[main] adgroup_today_spend:',adgroup_today_spend, ' adgroup_now_should_spend:' , adgroup_now_should_spend)
            
            if last_interval_spend <= MINIMUM_SPEND:
                print('last time interval spend too low, no adjustment')
                continue
            # Retrieve device seperated report
            lifetime_insights = collector_ad_group.get_adgroup_insights(date_preset='ALL_TIME', by_device=True)
            df_lifetime_insights = pd.DataFrame(lifetime_insights)
            df_lifetime_insights.loc[
                (df_lifetime_insights['conversions'] == 0)&(df_lifetime_insights['device'].str.contains("Mobile|Computers")), 'conversions'] = 1
            df_lifetime_insights = df_lifetime_insights[['device', 'spend', 'conversions']]

            # Check whether to adjust bid modifier
            all_device_target = df_lifetime_insights['conversions'].sum()
            all_device_spend = df_lifetime_insights['spend'].sum()
            mobile_target = df_lifetime_insights['conversions'][df_lifetime_insights['device'].str.contains("Mobile|Tablets")].sum()
            desktop_target = df_lifetime_insights['conversions'][df_lifetime_insights['device'].str.contains("Computers")].sum()
            mobile_spend = df_lifetime_insights['spend'][df_lifetime_insights['device'].str.contains("Mobile|Tablets")].sum()
            desktop_spend = df_lifetime_insights['spend'][df_lifetime_insights['device'].str.contains("Computers")].sum()
            
            with np.errstate(divide='ignore', invalid='ignore'):
                desktop_conversion_ratio = np.true_divide(desktop_target, all_device_target)
                mobile_conversion_ratio = np.true_divide(mobile_target, all_device_target)
                
                desktop_spend_ratio = np.true_divide(desktop_spend, all_device_spend)
                mobile_spend_ratio = np.true_divide(mobile_spend, all_device_spend)
                
                desktop_conversion_ratio = np.nan_to_num(desktop_conversion_ratio)
                mobile_conversion_ratio = np.nan_to_num(mobile_conversion_ratio)

                desktop_spend_ratio = np.nan_to_num(desktop_spend_ratio)
                mobile_spend_ratio = np.nan_to_num(mobile_spend_ratio)

                spend_ratio = np.true_divide(desktop_spend_ratio, mobile_spend_ratio)
                conversion_ratio = np.true_divide(desktop_conversion_ratio, mobile_spend_ratio)

            print('[main] desktop_conversion_ratio:', desktop_conversion_ratio,' mobile_conversion_ratio:', mobile_conversion_ratio)
            print('[main] desktop_spend_ratio:', desktop_spend_ratio,' mobile_spend_ratio:', mobile_spend_ratio)
            print('[main] spend_ratio', spend_ratio, ' converison_ratio', conversion_ratio)
            
            if spend_ratio < conversion_ratio:
                # desktop spend does not keep up
                print('[main] Make desktop spend more')
                bid_modifier_adjust(controller_ad_group, 'Desktop', 'up')
                bid_modifier_adjust(controller_ad_group, 'HighEndMobile', 'down')
                bid_modifier_adjust(controller_ad_group, 'Tablet', 'down')
                
            elif spend_ratio > conversion_ratio:
                # mobile spend does not keep up
                print('[main] Make mobile spend more')
                bid_modifier_adjust(controller_ad_group, 'Desktop', 'down')
                bid_modifier_adjust(controller_ad_group, 'HighEndMobile', 'up')
                bid_modifier_adjust(controller_ad_group, 'Tablet', 'up')


# In[ ]:


if __name__=='__main__':
    main()


# In[ ]:


# !jupyter nbconvert --to script handle_device_proportion.ipynb


# In[ ]:




