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
from bid_operator import reverse_bid_amount
import adgeek_permission as permission
import database_controller
import google_adwords_controller as controller
import google_adwords_report_generator as collector


# In[ ]:


CAMPAIGN_OBJECTIVE_FIELD = {
    'LINK_CLICKS': 'clicks',
    'CONVERSIONS':'conversions',
    'cpc': 'clicks',
    'cpa':'conversions',
}
BIDDING_COLUMN = {
    'CpmBid': 'cpm_bid',
    'CpvBid': 'cpv_bid', 
    'CpcBid': 'cpc_bid', 
    'TargetCpa': 'cpa_bid'
}


# In[ ]:


class Field:
    target_type = 'target_type'
    target = 'target'
    cost_per_target = 'cost_per_target'
    charge_type = 'charge_type'
    start_time = 'start_time'
    stop_time = 'stop_time'
    ai_start_date = 'ai_start_date'
    ai_stop_date = 'ai_stop_date'
    period = 'period'
    daily_budget = 'daily_budget'
    bid_amount = 'bid_amount'
    account_id = 'account_id'
    actions = 'actions'
    adgroup_id = 'adgroup_id'
    campaign_id = 'campaign_id'
    clicks = 'clicks'


# In[ ]:


class DatePreset:
    today = 'TODAY'
    yesterday = 'YESTERDAY'
    lifetime = 'ALL_TIME'
    last_14_days = 'LAST_14_DAYS'


# In[ ]:


def make_addition_dict(customer_id,
                       campaign_id,
                       channel_type,
                       status,
                       ai_status,
                       destination_type, 
                       is_optimized,
                       optimized_date,
                       cost_per_target,
                       daily_target,
                       daily_budget,
                       destination,
                       destination_max,
                       period,
                       period_left,
                       spend,
                       ai_spend_cap,
                       ai_start_date,
                       ai_stop_date,
                       spend_cap,
                       start_time,
                       stop_time,
                       target,
                       target_left,
                       bidding_type,
                       impressions,
                       ctr,
                       clicks,
                       conversions,
                       view_conversions,
                       cost_per_click,
                       cost_per_conversion,
                       all_conversions,
                       cost_per_all_conversion,
                       is_smart_spending,
                       is_target_suggest,
                       is_lookalike,
                       is_creative_opt,
                       is_device_pro_rata):
    
#     print(campaign)
    addition_dict = {}
    
    target = eval(CAMPAIGN_OBJECTIVE_FIELD[ destination_type ])
    target_left = int(destination) - eval(CAMPAIGN_OBJECTIVE_FIELD[ destination_type ])
    daily_target = target_left / period_left
    
    addition_dict['target'] = target
    addition_dict['target_left'] = target_left
    addition_dict['daily_target'] = daily_target
    
    addition_dict['period'] = ( ai_stop_date - ai_start_date ).days + 1
    addition_dict['period_left'] = ( ai_stop_date-datetime.datetime.now().date() ).days + 1
    return addition_dict


# In[ ]:


def data_collect(database_gdn, campaign):
    customer_id = campaign.get("customer_id")
    campaign_id = campaign.get("campaign_id")
    destination = campaign.get("destination")
    destination_type = campaign.get("destination_type")
    ai_start_date = campaign.get("ai_start_date")
    ai_stop_date = campaign.get("ai_stop_date")

    service_container = controller.AdGroupServiceContainer(customer_id=customer_id)
    controller_campaign = controller.Campaign(service_container=service_container, campaign_id=campaign_id)
    
    collector_campaign = collector.CampaignReportGenerator(campaign_id, media='gdn')
    campaign_lifetime_insights = collector_campaign.get_insights(date_preset=None)
    if not campaign_lifetime_insights:
        return
    addition_dict = make_addition_dict(**{**campaign, **campaign_lifetime_insights[0]})
    
    campaign_dict = {
        **campaign_lifetime_insights[0],
        **addition_dict,
    }
    database_gdn.upsert("campaign_target", campaign_dict)
    ad_group_list = controller_campaign.get_ad_groups()
    
    collector_ad_group = collector.AdGroupReportGenerator(campaign_id, media='gdn')
    ad_group_day_insights = collector_ad_group.get_insights(date_preset='TODAY')
    
    for insights in ad_group_day_insights:
        database_gdn.insert("adgroup_insights", insights)
        insights['bid_amount'] = math.ceil(reverse_bid_amount(insights[BIDDING_COLUMN[collector_ad_group.col]]))
        database_gdn.insert_ignore("adgroup_initial_bid", 
                                   {key : insights[key] for key in ["campaign_id", "adgroup_id", "bid_amount"]})
    database_gdn.dispose()


# In[ ]:


def main():
    start_time = datetime.datetime.now()
    db = database_controller.Database()
    database_gdn = database_controller.GDN(db)
    campaign_running_list = database_gdn.get_running_campaign().to_dict('records')
    print([campaign['campaign_id'] for campaign in campaign_running_list])
    for campaign in campaign_running_list:
        print('[campaign_id]: ', campaign.get('campaign_id'))
        
        data_collect( database_gdn, campaign )

    print(datetime.datetime.now()-start_time)


# In[ ]:


if __name__=='__main__':
    main()
#     df_campaign = data_collect(camp.customer_id, camp.campaign_id, 10000, camp.destination_type)


# In[ ]:


# !jupyter nbconvert --to script gdn_datacollector.ipynb


# In[ ]:




