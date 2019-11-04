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

import database_controller
import facebook_datacollector as datacollector


# In[ ]:


def make_campaign_bid(campaign_id):
    df_adsets = database_fb.retrieve("table_insights", campaign_id=campaign_id,)
    campaign_bid = df_adsets.sort_values('request_time').groupby('adset_id').tail(1).mean()['bid_amount']
    return campaign_bid

def main_retrieve():
    global database_fb
    database_fb = database_controller.FB(database_controller.Database)
    branding_campaign_list = database_fb.get_branding_campaign().to_dict('records')
    print([campaign.get('campaign_id') for campaign in branding_campaign_list])
    for campaign in branding_campaign_list:
        destination = campaign.get('destination')
        spend_cap   = campaign.get('ai_spend_cap')
        bid_amount  = spend_cap/destination

        campaign_bid = make_campaign_bid(campaign_id)

        campaign = datacollector.Campaigns(campaign_id,)
        insights = campaign.get_campaign_insights(date_preset='today')
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




