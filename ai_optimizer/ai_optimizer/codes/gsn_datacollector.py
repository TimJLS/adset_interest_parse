#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from googleads import adwords
import pandas as pd
import numpy as np
import math
import datetime
import database_controller
import google_adwords_controller as controller
import gdn_datacollector as datacollector
from bid_operator import reverse_bid_amount
import adgeek_permission as permission
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
}


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
                       is_device_pro_rata,
                       average_position,
                       top_impression_percentage):
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


def data_collect(database, campaign):
    customer_id = campaign.get("customer_id")
    campaign_id = campaign.get("campaign_id")
    destination = campaign.get("destination")
    destination_type = campaign.get("destination_type")
    ai_start_date = campaign.get("ai_start_date")
    ai_stop_date = campaign.get("ai_stop_date")
    
    collector_campaign = collector.CampaignReportGenerator(campaign_id, 'gsn')
    campaign_lifetime_insights = collector_campaign.get_insights(date_preset=None)
    if not campaign_lifetime_insights:
        return
    addition_dict = make_addition_dict(**{**campaign, **campaign_lifetime_insights[0]})
    
    campaign_dict = {
        **campaign_lifetime_insights[0],
        **addition_dict,
    }
    database.upsert("campaign_target", campaign_dict)
    
    collector_keyword = collector.KeywordReportGenerator(campaign_id, 'gsn')
    keyword_day_insights = collector_keyword.get_insights(date_preset='TODAY')
    for insights in keyword_day_insights:
        database.insert("keywords_insights", insights)
        if collector_keyword.bidding_type in BIDDING_COLUMN.keys():
            insights['bid_amount'] = math.ceil(reverse_bid_amount(insights[BIDDING_COLUMN[collector_keyword.col]]))
            database.insert_ignore("adgroup_initial_bid", 
                                   {key : insights[key] for key in ["campaign_id", "adgroup_id", "keyword_id", "bid_amount"]})
        else:
            continue
    database.dispose()


# In[ ]:


def main():
    start_time = datetime.datetime.now()
    db = database_controller.Database()
    database_gsn = database_controller.GSN(db)
    campaign_running_list = database_gsn.get_running_campaign().to_dict('records')
    print([campaign['campaign_id'] for campaign in campaign_running_list])
    for campaign in campaign_running_list:
        print('[campaign_id]: ', campaign.get('campaign_id'))
        
        data_collect(database_gsn, campaign)
    print(datetime.datetime.now()-start_time)


# In[ ]:


if __name__=='__main__':
    main()
#     data_collect(customer_id=CUSTOMER_ID, campaign_id=CAMPAIGN_ID)


# In[ ]:


# !jupyter nbconvert --to script gsn_datacollector.ipynb


# In[ ]:




