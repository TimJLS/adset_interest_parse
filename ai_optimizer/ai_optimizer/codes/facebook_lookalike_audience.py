#!/usr/bin/env python
# coding: utf-8

# In[34]:


import facebook_business.adobjects.adset as facebook_business_adset
import facebook_business.adobjects.campaign as facebook_business_campaign
import facebook_business.adobjects.adaccount as facebook_business_adaccount
import facebook_business.adobjects.customaudience as facebook_business_custom_audience

from facebook_business.api import FacebookAdsApi
import facebook_datacollector as collector
import mysql_adactivity_save as mysql_saver

import pandas as pd
import datetime
my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBALrHTgMWgRujnWcZA3ZB823phs6ynDDtQxnzIZASyRQZCHfr5soXBZA7NM9Dc4j9O8FtnlIzxiPCsYt4tmPQ6ZAT3yJLPuYQqjnWZBWX5dsOVzNhEqsHYj1jVJ3RAVVueW7RSxRDbNXKvK3W23dcAjNMjxIjQGIOgZDZD'

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)


# In[26]:


def get_lookalike_not_used(campaign_id):
    # check has already process this campaign first
    save_campaign_pixel_id(campaign_id)
    
    df_not_processed_lookalike = get_not_processed_lookalike_df(campaign_id)
    if df_not_processed_lookalike.empty:
        print('[get_lookalike_not_used] df_saved_pixel_id None')
        return
    print('[get_lookalike_not_used] df_saved_pixel_id len:', len(df_not_processed_lookalike))
    print(df_not_processed_lookalike.behavior_type.tolist())
    print('--')
    return df_not_processed_lookalike


# In[ ]:


def get_existing_lookalike(campaign_id):
    
    return


# In[30]:


def modify_result_db(campaign_id, lookalike_audience_id, is_lookalike_in_adset):
    #get date
    opt_date = datetime.datetime.now()
    #insert to table date and Ture for is_opt
    sql = "update campaign_pixel_id set is_lookalike_in_adset='{}', updated_at='{}' where campaign_id={} and lookalike_audience_id={}".format(is_lookalike_in_adset, opt_date, campaign_id, lookalike_audience_id)
    mydb = mysql_saver.connectDB(mysql_saver.DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute(sql)
    mydb.commit()
    mydb.close()


# In[32]:


def get_lookalike_audience_id(campaign_id):
    lookalike_audience_dict = dict()
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = "SELECT behavior_type, lookalike_audience_id FROM campaign_pixel_id WHERE campaign_id={} AND is_lookalike_in_adset='False'".format(campaign_id)
    my_cursor.execute(sql)
    results = my_cursor.fetchall()
    for (behavior_type, audience_id) in results:
        lookalike_audience_dict[behavior_type] = audience_id
    my_db.commit()
    my_cursor.close()
    my_db.close()
    return lookalike_audience_dict


# In[2]:


def is_lookalike_created(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = 'SELECT * FROM campaign_pixel_id where campaign_id = {}'.format(campaign_id)
    my_cursor.execute(sql)
    result = my_cursor.fetchall()
    my_db.commit()
    my_cursor.close()
    my_db.close()
    return len(result) > 0


# In[3]:


def create_lookalike_custom_audience(account_id, campaign_id, behavior_type, audience_id):

    lookalike = facebook_business_custom_audience.CustomAudience(parent_id='act_'+account_id)
    lookalike.update({
        facebook_business_custom_audience.CustomAudience.Field.name: 'My lookalike audience for {}'.format(behavior_type),
        facebook_business_custom_audience.CustomAudience.Field.subtype: facebook_business_custom_audience.CustomAudience.Subtype.lookalike,
        facebook_business_custom_audience.CustomAudience.Field.origin_audience_id: audience_id,
        facebook_business_custom_audience.CustomAudience.Field.lookalike_spec: {
            'type': 'similarity',
            'country': 'TW',
        },
    })
    resp = lookalike.remote_create()
    lookalike_audience_id = resp.get("id")
    print('==========[lookalike response]')
    print(resp)
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor(buffered=True)
    update_sql = ("UPDATE campaign_pixel_id SET is_created='{}', lookalike_audience_id={} WHERE campaign_id={} AND behavior_type='{}'".format( True, lookalike_audience_id, campaign_id, behavior_type ) )
    my_cursor.execute(update_sql)
    my_db.commit()
    
    return


# In[4]:


def create_campaign_custom_audience_by_pixel(campaign_id):
    campaign = facebook_business_campaign.Campaign(campaign_id)
    campaign_object = campaign.get_insights(
        fields = [facebook_business_campaign.Campaign.Field.account_id] )
    account_id = campaign_object[0].get("account_id")    
    
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor(buffered=True)
    sql = "SELECT * FROM campaign_pixel_id WHERE campaign_id = {} AND is_created='False'".format(campaign_id)
    df_not_opted_pixel_id = pd.read_sql(sql, con=my_db)
    df_not_opted_pixel_id = df_not_opted_pixel_id.dropna(subset=['pixel_id'])
    if df_not_opted_pixel_id.empty:
        print('[create_campaign_custom_audience_by_pixel]: all custom audience is created.')
        return
    for pixel_id in df_not_opted_pixel_id['pixel_id'].unique().tolist():
        event_sources = [{
            "id": pixel_id,
            "type": "pixel"
        }]
        for behavior_type in df_not_opted_pixel_id['behavior_type'].tolist():
            filters = [{
                "field": "event",
                "operator": "=",
                "value": behavior_type,
            }]
            filter = {
                "operator": "or",
                "filters": filters,
            }
            fields = []
            params = {
                'name': 'My {} Custom Audience'.format(behavior_type),
                "operator" : "and",
                'rule': {
                    "inclusions": {
                        "operator": "or",
                        "rules":[{
                            "event_sources": event_sources, 
                            "retention_seconds" : 2592000,
                            "filter": filter,
                            "aggregation" : {
                                "type":"count",
                                "operator":">",
                                "value":1
                            }
                        }]
                    }
                }
            }
            resp = facebook_business_adaccount.AdAccount('act_'+account_id).create_custom_audience(
                fields=fields,
                params=params,
            )
            print('==================[custom]')
            print(resp)
            audience_id = resp.get("id")
            
            create_lookalike_custom_audience(account_id, campaign_id, behavior_type, audience_id)
            
            update_sql = ("UPDATE campaign_pixel_id SET is_created='{}', audience_id={} WHERE campaign_id={} AND behavior_type='{}'".format( True, audience_id, campaign_id, behavior_type ) )
            my_cursor.execute(update_sql)
            my_db.commit()
    my_cursor.close()
    my_db.close()
    return


# In[18]:


def get_not_processed_lookalike_df(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    sql = "SELECT * FROM campaign_pixel_id WHERE campaign_id={} AND is_lookalike_in_adset='False'".format(campaign_id)
    df_saved_pixel_id = pd.read_sql(sql, con=my_db)
    my_db.close()
    return df_saved_pixel_id


# In[17]:


def get_processed_lookalike_df(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    sql = "SELECT * FROM campaign_pixel_id WHERE campaign_id={} AND is_lookalike_in_adset='True'".format(campaign_id)
    df_saved_pixel_id = pd.read_sql(sql, con=my_db)
    my_db.close()
    return df_saved_pixel_id


# In[10]:


def save_campaign_pixel_id(campaign_id):
    CONVERSION_BEHAVIOR_LIST = ['Purchase', 'AddToCart', 'ViewContent']
    campaign = facebook_business_campaign.Campaign(campaign_id)
    campaign_object = campaign.get_ad_sets(
        fields = [facebook_business_adset.AdSet.Field.id, facebook_business_adset.AdSet.Field.status,] )
    active_adset_id_list = [adset_object.get("id") for adset_object in campaign_object if adset_object.get("status")=='ACTIVE']
    adset = facebook_business_adset.AdSet(int(active_adset_id_list[0]))
    adset_object = adset.remote_read( fields=['promoted_object'] )
    promoted_object = adset_object.get("promoted_object")
    pixel_id = promoted_object.get("pixel_id") if promoted_object else None
    if pixel_id is None:
        return
    
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    for behavior_type in CONVERSION_BEHAVIOR_LIST:
        print('[process_campaign_funnel_lookalike]', campaign_id, behavior_type, pixel_id, 'False')
        sql = "INSERT IGNORE INTO campaign_pixel_id ( campaign_id, behavior_type, pixel_id, is_created ) VALUES ( %s, %s, %s, %s )"
        val = ( campaign_id, behavior_type, pixel_id, 'False')
        my_cursor.execute(sql, val)
        my_db.commit()    
    my_cursor.close()
    my_db.close()
    return


# In[11]:


def save_pixel_id_for_all_campaign():
    running_campaign_id_list = mysql_saver.get_campaign_target().campaign_id.tolist()
    conversion_campaign_id_list = mysql_saver.get_running_conversion_campaign().campaign_id.tolist()
    print('[save_pixel_id_for_all_campaign] current running campaign:', len(running_campaign_id_list), running_campaign_id_list )
    for campaign_id in running_campaign_id_list:
        print('[save_pixel_id_for_all_campaign] campaign_id:', campaign_id)
        save_campaign_pixel_id(campaign_id)
    print('[save_pixel_id_for_all_campaign] current conversion campaign:', len(running_campaign_id_list), running_campaign_id_list )
    for campaign_id in conversion_campaign_id_list:
        print('[save_pixel_id_for_all_campaign] conveersion campaign_id:', campaign_id)
        create_campaign_custom_audience_by_pixel(campaign_id)


# In[8]:


def main():
    save_pixel_id_for_all_campaign()


# In[9]:


if __name__ == "__main__":
    main()


# In[33]:


#!jupyter nbconvert --to script facebook_lookalike_audience.ipynb


# In[ ]:




