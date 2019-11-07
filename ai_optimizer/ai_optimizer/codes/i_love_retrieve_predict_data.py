#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import datetime

import adgeek_permission as permission
import database_controller
import facebook_datacollector as datacollector


# In[ ]:


def make_campaign_bid(campaign_id):
    df_adsets = database_fb.retrieve("table_insights", campaign_id=campaign_id,)
    campaign_bid = df_adsets.sort_values('request_time').groupby('adset_id').tail(1).mean()['bid_amount']
    return campaign_bid.astype(object)

def main_retrieve():
    global database_fb
    database_fb = database_controller.FB(database_controller.Database)
    branding_campaign_list = database_fb.get_branding_campaign().to_dict('records')
    print([campaign.get('campaign_id') for campaign in branding_campaign_list])
    for campaign in branding_campaign_list:
        account_id = campaign.get('account_id')
        campaign_id = campaign.get('campaign_id')
        destination = campaign.get('destination')
        spend_cap   = campaign.get('ai_spend_cap')
        bid_amount  = spend_cap/destination
        print('=========[campaign_id]:', campaign_id)
        permission.init_facebook_api(account_id)
        campaign_bid = make_campaign_bid(campaign_id)

        campaign = datacollector.Campaigns(campaign_id,)
        insights = campaign.get_campaign_insights(date_preset='today')
        insights['target'], _, _, _ = insights.pop('action'), insights.pop('awareness'), insights.pop('desire'), insights.pop('interest')
        
        insights['cost_per_target'] = int(insights['spend'])/insights['target'] if insights.get('target') else 0
        print(insights)
        if insights:
            insights['campaign_id'] = campaign_id
            insights['bid_amount'] = campaign_bid
            database_fb.upsert("campaign_insights", insights)


# In[ ]:


if __name__ == '__main__':
    main_retrieve()


# In[ ]:


# !jupyter nbconvert --to script i_love_retrieve_predict_data.ipynb


# In[ ]:




