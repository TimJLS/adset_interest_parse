#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gdn_datacollector
from gdn_datacollector import Campaign
from gdn_datacollector import AdGroup
from gdn_datacollector import DatePreset
import google_adwords_controller as controller
import gdn_db
import datetime
from googleads import adwords
import pandas as pd
import copy
import math

IS_DEBUG = False
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
DATABASE = "dev_gdn"
DATE = datetime.datetime.now().date()
DATETIME = datetime.datetime.now()
AGE_RANGE_LIST = [503001,503002,503003,503004,503005,503006,503999,]


# In[2]:


def get_campaign_custom_audience(campaign_id):
    all_converters_dict_list = list()
    optimized_list_dict_list = list()
    custom_audience_dict = dict()
    
    df = gdn_db.get_table(campaign_id=campaign_id, table='custom_audience')
    df_optimized_list = df[ (df.behavior_type=='optimized_list') & (df.list_type!='similar') ]
    df_similar_all_converters = df[ (df.behavior_type=='all_converters') & (df.list_type=='similar') ]
    
    if df.empty:
        print('[get_custom_audience_id]: No custom audience is created.')
    if df_optimized_list.empty:
        print('optimized list is not created.')
    else:
        for idx, row in df_optimized_list.iterrows():
            custom_audience_dict['behavior_type'] = row['behavior_type']
            custom_audience_dict['list_type'] = row['list_type']
            custom_audience_dict['criterion_id'] = row['criterion_id']
            optimized_list_dict_list.append(copy.deepcopy(custom_audience_dict))
    if df_similar_all_converters.empty:
        print('similar all converters is not created.')
    else:
        for idx, row in df_similar_all_converters.iterrows():
            custom_audience_dict['behavior_type'] = row['behavior_type']
            custom_audience_dict['list_type'] = row['list_type']
            custom_audience_dict['criterion_id'] = row['criterion_id']
            all_converters_dict_list.append(copy.deepcopy(custom_audience_dict))
    return optimized_list_dict_list, all_converters_dict_list
    


# In[3]:


def get_ad_groups_include_custom_audience(customer_id, campaign_id):
    group_service = controller.AdGroupServiceContainer(customer_id=customer_id)
    campaign = controller.Campaign(group_service, campaign_id=campaign_id)
    campaign.get_ad_groups()
    ad_group_list = campaign.ad_groups
    user_list_criterion_list = []
    for ad_group in ad_group_list:
        biddable_criterions, negative_criterions = ad_group.user_list_criterions.retrieve()
    #     print([biddable_criterion['userListId'] for biddable_criterion in biddable_criterions])
        user_list_criterion_list.append( {
            'criterion_id': [biddable_criterion['userListId'] for biddable_criterion in biddable_criterions],
            'ad_group_id': ad_group.ad_group_id
        })
    return user_list_criterion_list


# In[4]:


def save_campaign_custom_audience(customer_id, campaign_id):
    adwords_client.SetClientCustomerId(customer_id)
    _FIELDS = ['Name', 'Status', 'AccessReason', 'AccountUserListStatus', 'AppId', 'ClosingReason',
               'ConversionTypes', 'DataSourceType', 'DataUploadResult', 'DateSpecificListEndDate', 'DateSpecificListRule',
               'DateSpecificListStartDate', 'Description', 'ExpressionListRule', 'IntegrationCode', 'IsEligibleForDisplay',
               'IsEligibleForSearch', 'IsReadOnly', 'ListType', 'MembershipLifeSpan', 'PrepopulationStatus', 'Rules', 'SeedListSize', 
               'SeedUserListDescription', 'SeedUserListId', 'SeedUserListName', 'SeedUserListStatus', 'Size', 'SizeForSearch',
               'SizeRange', 'SizeRangeForSearch', 'UploadKeyType']
    selector = [{
        'fields': _FIELDS,
        'predicates': [{
            'field': 'Status',
            'operator': 'EQUALS',
            'values': ['OPEN']   
        }]
    }]
    ad_group_service = adwords_client.GetService( 'AdwordsUserListService', version='v201809' )
    ad_group_criterion = ad_group_service.get(selector)
    entries = ad_group_criterion['entries']
    custom_audience_list = [ entry for entry in entries if 'optimized list' in entry['name'] or 'All Converter' in entry['name'] ]
    
    custom_criterion_list = []
    user_list_criterion_list = get_ad_groups_include_custom_audience(customer_id, campaign_id)     
    for custom_audience in custom_audience_list:
        custom_criterion = {}
        custom_criterion['customer_id'] = customer_id
        custom_criterion['criterion_id'] = custom_audience['id']
        custom_criterion['campaign_id'] = campaign_id
        custom_criterion['behavior_type'] = 'optimized_list' if 'optimized list' in custom_audience['name'] else 'all_converters'
        custom_criterion['list_type'] = custom_audience['listType'].lower() if custom_audience['listType'] else None
        custom_criterion['display_name'] = custom_audience['name']
        custom_criterion['size'] = custom_audience['size']
        custom_criterion['membership_life_span'] = custom_audience['membershipLifeSpan']
        custom_criterion['seed_id'] = custom_audience['seedUserListId'] if custom_audience['listType']=='SIMILAR' else None
        custom_criterion['seed_size'] = custom_audience['seedListSize'] if custom_audience['listType']=='SIMILAR' else None
        custom_criterion['ad_group_ids'] = []
        for user_list_criterion in user_list_criterion_list:
            for criterion_id in user_list_criterion['criterion_id']:
                if criterion_id == custom_audience['id']:
                    custom_criterion['ad_group_ids'].append(user_list_criterion['ad_group_id'])
        custom_criterion_list.append(custom_criterion)
    my_db = gdn_db.connectDB(gdn_db.DATABASE)
    my_cursor = my_db.cursor()
    for custom_criterion in custom_criterion_list:
        customer_id = custom_criterion['customer_id']
        campaign_id = custom_criterion['campaign_id']
        ad_group_ids = ','.join(str(ad_group_id) for ad_group_id in custom_criterion['ad_group_ids'])
        criterion_id = custom_criterion['criterion_id']
        behavior_type = custom_criterion['behavior_type']
        list_type = custom_criterion['list_type']
        display_name = custom_criterion['display_name']
        size = custom_criterion['size']
        membership_life_span = custom_criterion['membership_life_span']
        seed_id = custom_criterion['seed_id']
        seed_size = custom_criterion['seed_size']
        sql = "INSERT IGNORE INTO custom_audience ( customer_id, campaign_id, ad_group_ids, criterion_id, behavior_type, list_type, display_name, size, membership_life_span, seed_id, seed_size ) VALUES ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s )"
        val = ( customer_id, campaign_id, ad_group_ids, criterion_id, behavior_type, list_type, display_name, size, membership_life_span, seed_id, seed_size )
        my_cursor.execute(sql, val)
        my_db.commit()    
    my_cursor.close()
    my_db.close()
    return custom_criterion_list


# In[5]:


def save_custom_audience_for_all_campaign(campaign_id=None):
    if campaign_id:
        df = gdn_db.get_campaign_target(campaign_id=campaign_id)[['customer_id', 'campaign_id']]
        if df.empty:
            print('[save_custom_audience_for_all_campaign]: campaign has ended.')
            return
        customer_id, campaign_id = df.customer_id.tolist()[0], df.campaign_id.tolist()[0]
        save_campaign_custom_audience(customer_id, campaign_id)
    else:
        running_campaign_id_list = gdn_db.get_campaign_target().campaign_id.tolist()
        df_conversion_campaign = gdn_db.get_performance_campaign_is_running()[['customer_id', 'campaign_id']]
        if df_conversion_campaign.empty:
            print('[save_custom_audience_for_all_campaign]: no running conversion campaigns.')
            return
        for idx, row in df_conversion_campaign.iterrows():
            customer_id, campaign_id = row.customer_id.tolist(), row.campaign_id.tolist()
            save_campaign_custom_audience(customer_id, campaign_id)
#         print('[save_custom_audience_for_all_campaign] current conversion campaign:', len(conversion_campaign_id_list), conversion_campaign_id_list )


# In[6]:


def get_current_ad_group_ids(campaign_id, criterion_id):
    sql = "select ad_group_ids from custom_audience where campaign_id={} and criterion_id={}".format(campaign_id, criterion_id)
    mydb = gdn_db.connectDB(gdn_db.DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    ad_group_ids = mycursor.fetchall()
    mydb.close()
    mycursor.close()
    return ad_group_ids[0][0].split(',')


# In[7]:


def modify_result_db(campaign_id, criterion_id, ad_group_id):
    ad_group_id_list = get_current_ad_group_ids(campaign_id, criterion_id)
    #get date
    opt_date = datetime.datetime.now()
    ad_group_id_list = ad_group_id_list + [str(ad_group_id)]
    ad_group_ids = ','.join(str(ad_group_id) for ad_group_id in ad_group_id_list)
    #insert to table date and Ture for is_opt
    sql = "update custom_audience set ad_group_ids='{}', updated_at='{}' where campaign_id={} and criterion_id={}".format(ad_group_ids, opt_date, campaign_id, criterion_id)
    mydb = gdn_db.connectDB(gdn_db.DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    mydb.commit()
    mydb.close()
    mycursor.close()


# In[8]:


def main():
    save_custom_audience_for_all_campaign()


# In[9]:


if __name__=='__main__':
    main()


# In[10]:


#!jupyter nbconvert --to script gdn_custom_audience.ipynb


# In[ ]:




