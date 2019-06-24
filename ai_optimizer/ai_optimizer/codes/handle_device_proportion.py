#!/usr/bin/env python
# coding: utf-8

# In[1]:


import uuid
from googleads import adwords
import sys
import pandas as pd
import numpy as np
import math
import datetime
from copy import deepcopy
import gdn_db
import gdn_datacollector as datacollector
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
DEVICE_BALANCE_PROPORTION = 0.5
TIME_RANGE = 3
MINIMUM_SPEND = 10
DEVICE_CRITERION = {
    'Desktop': 30000,
    'HighEndMobile': 30001,
    'ConnectedTv': 30004,
    'Tablet': 30002,
}

# In[2]:
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
        print('[resp]: ', resp)
    return resp

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


# In[3]:


def bid_modifier_adjust(ad_group_id, device_target, direction):
    resps = retrieve_bid_modifier(adwords_client, ad_group_id)
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
    print('[ad_group_id]: ', ad_group_id)
    print('[bid_modifier_dict]: ', bid_modifier_dict)
    resp = assign_bid_modifier(adwords_client, ad_group_id, **bid_modifier_dict)
    return resp


# In[15]:


def main():
    df_performance_campaign = gdn_db.get_performance_campaign_is_running()
    for campaign_id in df_performance_campaign['campaign_id'].tolist():
        print('[current time]: ', datetime.datetime.now())
        print('[campaign_id]: ', campaign_id)
        customer_id = df_performance_campaign['customer_id'][df_performance_campaign.campaign_id==campaign_id].iloc[0]
        adwords_client.SetClientCustomerId(customer_id)
        camp = datacollector.Campaign(customer_id=customer_id, campaign_id=campaign_id, destination_type='CONVERSIONS')
        ad_group_id_list = camp.get_adgroup_id_list()
        for ad_group_id in ad_group_id_list:
            print('[ad_group_id]: ', ad_group_id)
            ad_group = datacollector.AdGroup(
                customer_id=customer_id, campaign_id=campaign_id, adgroup_id=ad_group_id, destination_type='CONVERSIONS')
            # Retrieve hourly seperated report
            hourly_insights = ad_group.get_adgroup_insights(adwords_client, date_preset='TODAY', by_hour=True)
            df_hourly_insights = pd.DataFrame(hourly_insights).sort_values(by=['hour_of_day']).reset_index(drop=True)  
            # Check last time interval spend is normal or not
            current_hour = datetime.datetime.now().hour
            last_interval_spend = df_hourly_insights[
                df_hourly_insights.hour_of_day.between(current_hour-TIME_RANGE, current_hour, inclusive=True)]['spend'].sum()
            if last_interval_spend <= MINIMUM_SPEND:
                print('last time interval spend too low, no adjustment')
                return
            # Retrieve device seperated report
            device_insights = ad_group.get_adgroup_insights(adwords_client, date_preset='TODAY', by_device=True)
            df_device_insights = pd.DataFrame(device_insights)
            df_mobile = df_device_insights[['device', 'spend']].sort_values(by=['spend'], ascending=False)[df_device_insights['device'].str.contains("Mobile")].reset_index(drop=True)
            df_desktop = df_device_insights[['device', 'spend']].sort_values(by=['spend'], ascending=False)[df_device_insights['device'].str.contains("Computers")].reset_index(drop=True)

            # Check whether to adjust bid modifier
            all_device_spend = df_device_insights['spend'].sum()
            with np.errstate(divide='ignore', invalid='ignore'):
                desktop_spend_ratio = np.true_divide(df_desktop['spend'].iloc[0], all_device_spend)
                mobile_spend_ratio = np.true_divide(df_mobile['spend'].iloc[0], all_device_spend)
                desktop_spend_ratio = np.nan_to_num(desktop_spend_ratio)
                mobile_spend_ratio = np.nan_to_num(mobile_spend_ratio)
            
            print('desktop_spend_ratio: ', desktop_spend_ratio,)
            print('mobile_spend_ratio: ', mobile_spend_ratio,)
            
            if desktop_spend_ratio < mobile_spend_ratio:
                print('desktop spend ratio low, direction up')
                bid_modifier_adjust(ad_group_id, 'Desktop', 'up')
                bid_modifier_adjust(ad_group_id, 'HighEndMobile', 'down')
            elif desktop_spend_ratio > mobile_spend_ratio:
                print('desktop spend ratio high, direction down')
                bid_modifier_adjust(ad_group_id, 'Desktop', 'down')
                bid_modifier_adjust(ad_group_id, 'HighEndMobile', 'up')


# In[2]:


if __name__=='__main__':
    main()


# In[3]:


#!jupyter nbconvert --to script handle_device_proportion.ipynb


# In[ ]:




