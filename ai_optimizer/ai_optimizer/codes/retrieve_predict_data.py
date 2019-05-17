#!/usr/bin/env python
# coding: utf-8

# In[11]:


import facebook_datacollector as datacollector
import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import datetime
DATABASE="dev_facebook_test"
HOST = "aws-prod-ai-private.adgeek.cc"
DATE = datetime.datetime.now().date()


# In[12]:


import mysql_adactivity_save as ad_save


# In[13]:




def make_campaign_bid(campaign_id):
    mydb = ad_save.connectDB(DATABASE)
    sub_sql = "select distinct adset_id from adset_insights where ( campaign_id = {}  and request_time like '{}%' )".format(campaign_id, DATE)
    adset_id_list = pd.read_sql(sub_sql, con=mydb)['adset_id'].unique()
    sql = "select adset_id, bid_amount, request_time from adset_insights where adset_id in ({})".format(sub_sql)
    df_test = pd.read_sql(sql, con=mydb)
    mydb.close()

    df_test.sort_values('request_time').groupby('adset_id').tail(1)
    campaign_bid = df_test.sort_values('request_time').groupby('adset_id').tail(1).mean()['bid_amount']
    return campaign_bid
def main_retrieve():
    mydb = ad_save.connectDB(DATABASE)
    df = ad_save.get_campaign_target()
    mydb.close()
    for campaign_id in df.campaign_id.tolist():
        destination = df[df.campaign_id==campaign_id].destination.iloc[0]
        spend_cap = df[df.campaign_id==campaign_id].spend_cap.iloc[0]
        bid_amount = spend_cap/destination
        charge_type = df[df.campaign_id==campaign_id].charge_type.iloc[0]

        campaign_bid = make_campaign_bid(campaign_id)

        campaign = datacollector.Campaigns(campaign_id, charge_type)
        insights = campaign.get_campaign_insights(date_preset='today')
        insights['campaign_id'] = campaign_id
        insights['bid_amount'] = campaign_bid


# In[14]:


if __name__ == '__main__':
    main_retrieve()


# In[8]:





# In[ ]:




