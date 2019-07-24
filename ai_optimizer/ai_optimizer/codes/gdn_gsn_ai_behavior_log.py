#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
import time
from googleads import adwords
import mysql.connector
import pandas as pd
import datetime
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)

import gdn_db as gdn_saver
import gsn_db as gsn_saver

class BehaviorType:
    COPY = 'copy'
    CLOSE = 'close'
    CREATE = 'create'
    ADJUST = 'adjust'
    OPEN = 'open'


# In[24]:


def get_adgroup_name_bidding(db_type, campaign_id, adgroup_id, criterion_id, criterion_type):
    ADGROUP_SERVICE_FIELDS = ['AdGroupId', 'Name', 'CpcBid', 'CampaignId']
    ADGROUP_CRITERION_SERVICE_FIELDS = ['AdGroupId', 'CpcBid', 'CriteriaType', 'UserInterestId', 'UserInterestName', 'UserListId', 'LabelIds']
    if criterion_type == 'audience':
        table = 'audience_insights'
        criterion = 'criterion_id'
        adgroup = 'adgroup_id'
        field_list = ADGROUP_CRITERION_SERVICE_FIELDS
        field = 'Id'
        service = 'AdGroupCriterionService'
        sql = "SELECT DISTINCT customer_id FROM {} WHERE {}={} AND {}={}".format(table, adgroup, adgroup_id, criterion, criterion_id)
    elif criterion_type == 'keyword':
        table = 'keywords_insights'
        criterion = 'keyword_id'
        adgroup = 'adgroup_id'
        field_list = ADGROUP_CRITERION_SERVICE_FIELDS
        field = 'Id'
        service = 'AdGroupCriterionService'
        sql = "SELECT DISTINCT customer_id FROM {} WHERE {}={} AND {}={}".format(table, adgroup, adgroup_id, criterion, criterion_id)
    elif criterion_type == 'adgroup':
        table = 'campaign_target'
        campaign = 'campaign_id'
        field_list = ADGROUP_SERVICE_FIELDS
        field = 'AdGroupId'
        service = 'AdGroupService'
        sql = "SELECT DISTINCT customer_id FROM {} WHERE {}={}".format(table, campaign, campaign_id)
    engine = create_engine( 'mysql://{}:{}@{}/{}'.format(gdn_saver.USER, gdn_saver.PASSWORD, gdn_saver.HOST, db_type) )
    with engine.connect() as conn, conn.begin():
        df = pd.read_sql(sql, con=conn)
        engine.dispose()
        customer_id = df.customer_id.iloc[0]
    adwords_client.SetClientCustomerId(customer_id)

    selector= [{
        'fields': field_list,
        'predicates': [{
            'field': field,
            'operator': 'EQUALS',
            'values':[criterion_id]
        }]
    }]
#     service = adwords_client.GetService('AdGroupService', version='v201809')
    adwords_service = adwords_client.GetService(service, version='v201809')
    entries = adwords_service.get(selector)['entries']
    if criterion_type == 'keyword':
        criterion = [ entry['criterion'] for i, entry in enumerate(entries) if entry['criterion']['type']=='KEYWORD' and entry['adGroupId']==adgroup_id ]
        bid_amount = [ entry['biddingStrategyConfiguration']['bids'][0]['bid']['microAmount'] for i, entry in enumerate(entries) if entry['criterion']['type']=='KEYWORD' and entry['adGroupId']==adgroup_id ][0]
        name = [ entry['criterion']['text'] for i, entry in enumerate(entries) if entry['criterion']['type']=='KEYWORD' and entry['adGroupId']==adgroup_id ][0]

    elif criterion_type == 'audience':
        criterion = [ entry['criterion'] for i, entry in enumerate(entries) if entry['criterion']['type']=='USER_INTEREST' and entry['adGroupId']==adgroup_id ]
        bid_amount = [ entry['biddingStrategyConfiguration']['bids'][0]['bid']['microAmount'] for i, entry in enumerate(entries) if entry['criterion']['type']=='USER_INTEREST' and entry['adGroupId']==adgroup_id ][0]
        name = [ entry['criterion']['userInterestName'] for i, entry in enumerate(entries) if entry['criterion']['type']=='USER_INTEREST' and entry['adGroupId']==adgroup_id ][0]
    
    elif criterion_type == 'adgroup':
        criterion = None
        bid_amount = [ entry['biddingStrategyConfiguration']['bids'][0]['bid']['microAmount'] for i, entry in enumerate(entries) ][0]
        name = entries[0]['name']

    return name, bid_amount/pow(10, 6)


# In[26]:


def save_adgroup_behavior(behavior_type, db_type, campaign_id, adgroup_id, criterion_id, criterion_type, behavior_misc = '' ):
    '''
    ad_group_pair = {'db_type': 'dev_gdn', 'adgroup_id': 71252991065, 'criterion_id': None, 'criterion_type': 'adgroup'}
    audience_pair = {'db_type': 'dev_gdn', 'adgroup_id': 71252991065, 'criterion_id': 164710527631, 'criterion_type': 'audience'}
    keywords_pair = {'db_type': 'dev_gsn', 'adgroup_id': 71353342785, 'criterion_id': 298175279711, 'criterion_type': 'keyword'}
    '''
    display_name , criterion_bid = get_adgroup_name_bidding(db_type, campaign_id, adgroup_id, criterion_id, criterion_type)
    created_at = int(time.time())

    if behavior_type == BehaviorType.ADJUST:
        if criterion_bid == behavior_misc:
            return
        behavior_misc = str(criterion_bid) + ':' + str(behavior_misc)

    my_db = gdn_saver.connectDB( db_type )
    my_cursor = my_db.cursor()
    sql = "INSERT INTO ai_behavior_log ( campaign_id, adgroup_id, criterion_id, display_name, criterion_type, behavior, behavior_misc, created_at ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s )"
    val = ( int(campaign_id), int(adgroup_id), criterion_id, display_name, criterion_type, behavior_type, behavior_misc, int(created_at) )
    my_cursor.execute(sql, val)
    my_db.commit()
    my_cursor.close()
    my_db.close()
    


# In[19]:


if __name__ == "__main__":
    ad_group_pair = {'db_type': 'dev_gdn', 'campaign_id': 1111, 'adgroup_id': 71252991065, 'criterion_id': None, 'criterion_type': 'adgroup'}
    audience_pair = {'db_type': 'dev_gdn', 'campaign_id': 1111, 'adgroup_id': 71252991065, 'criterion_id': 164710527631, 'criterion_type': 'audience'}
    keywords_pair = {'db_type': 'dev_gsn', 'campaign_id': 1111, 'adgroup_id': 71353342785, 'criterion_id': 298175279711, 'criterion_type': 'keyword'}

    save_adgroup_behavior(BehaviorType.CREATE, **ad_group_pair)
#     save_adgroup_behavior(BehaviorType.ADJUST, **audience_pair, behavior_misc=15)
#     save_adgroup_behavior(BehaviorType.ADJUST, **keywords_pair, behavior_misc=15)
#     print(adset_name , adset_bid )"


# In[25]:


#!jupyter nbconvert --to script gdn_gsn_ai_behavior_log.ipynb


# In[ ]:




