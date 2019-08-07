#!/usr/bin/env python
# coding: utf-8

# In[3]:


import facebook_business.adobjects.adset as facebook_business_adset
import facebook_business.adobjects.campaign as facebook_business_campaign
import facebook_business.adobjects.adaccount as facebook_business_adaccount
import facebook_business.adobjects.customaudience as facebook_business_custom_audience

import facebook_datacollector as collector
import mysql_adactivity_save as mysql_saver
import adgeek_permission as permission

import pandas as pd
import datetime
import time



CONVERSION_BEHAVIOR_LIST = ['Purchase', 'AddToCart', 'ViewContent']


# In[4]:


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


# In[5]:


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
    mycursor.close()


# In[6]:


def get_custom_audience_id(campaign_id):
    custom_audience_dict = dict()
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = "SELECT behavior_type, audience_id FROM campaign_pixel_id WHERE campaign_id={} AND is_created='True'".format(campaign_id)
    my_cursor.execute(sql)
    results = my_cursor.fetchall()
    my_db.commit()
    my_cursor.close()
    my_db.close()
    
    if len(results) == 0:
        print('[get_custom_audience_id]: No custom audience is created.')
        return 
    for (behavior_type, audience_id) in results:
        custom_audience_dict[behavior_type] = audience_id

    return custom_audience_dict


# In[50]:


def get_lookalike_audience_id(campaign_id):
    import collections
    lookalike_audience_dict = collections.OrderedDict()
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    
    sql = "SELECT behavior_type, lookalike_audience_id FROM campaign_pixel_id WHERE campaign_id={}".format(campaign_id)
    my_cursor.execute(sql)
    results = my_cursor.fetchall()
    
    is_in_adset_sql = "SELECT behavior_type, lookalike_audience_id FROM campaign_pixel_id WHERE campaign_id={} AND is_lookalike_in_adset='False' ORDER BY approximate_count DESC".format(campaign_id)
    my_cursor.execute(is_in_adset_sql)
    is_in_adset_results = my_cursor.fetchall()
    
    if len(results) == 0:
        print('[get_lookalike_audience_id]: No lookalike audience is created.')
    elif len(is_in_adset_results) == 0:
        print('[get_lookalike_audience_id]: All lookalike audience is in adset.')
    
    for (behavior_type, audience_id) in is_in_adset_results:
        lookalike_audience_dict[behavior_type] = audience_id

    return lookalike_audience_dict


# In[6]:


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


# In[7]:


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
    try:
        resp = lookalike.remote_create()
    except Exception as e:
        print('[create_lookalike_custom_audience]: unexpected error occired.')
        my_cursor.close()
        my_db.close()
        return
    lookalike_audience_id = resp.get("id")
    print('==========[lookalike response]')
    print(resp)
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor(buffered=True)
    update_sql = ("UPDATE campaign_pixel_id SET is_created='{}', lookalike_audience_id={} WHERE campaign_id={} AND behavior_type='{}'".format( True, lookalike_audience_id, campaign_id, behavior_type ) )
    my_cursor.execute(update_sql)
    my_db.commit()
    my_cursor.close()
    my_db.close()
    return


# In[8]:


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
        print('[create_campaign_custom_audience_by_pixel]: all custom audience of campaign {} is created.'.format(campaign_id))
        if is_lookalike_audience_created(campaign_id):
            print('[create_campaign_custom_audience_by_pixel]: all lookalike audience of campaign {} is created.'.format(campaign_id))
        else:
            print('[create_campaign_custom_audience_by_pixel]: all lookalike audience of campaign {} is not set.'.format(campaign_id))
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
            try:
                resp = facebook_business_adaccount.AdAccount('act_'+account_id).create_custom_audience(
                    fields=fields,
                    params=params,
                )
            except Exception as e:
                print('[create_campaign_custom_audience_by_pixel]: unexpected error occured.')
                my_cursor.close()
                my_db.close()
                return
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


# In[9]:


def get_not_processed_lookalike_df(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    sql = "SELECT * FROM campaign_pixel_id WHERE campaign_id={} AND is_lookalike_in_adset='False'".format(campaign_id)
    df_saved_pixel_id = pd.read_sql(sql, con=my_db)
    my_db.close()
    return df_saved_pixel_id


# In[10]:


def get_processed_lookalike_df(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    sql = "SELECT * FROM campaign_pixel_id WHERE campaign_id={} AND is_lookalike_in_adset='True'".format(campaign_id)
    df_saved_pixel_id = pd.read_sql(sql, con=my_db)
    my_db.close()
    return df_saved_pixel_id


# In[11]:


def save_campaign_pixel_id(campaign_id):
    campaign = facebook_business_campaign.Campaign(campaign_id)
    campaign_object = campaign.get_ad_sets(
        fields = [facebook_business_adset.AdSet.Field.id, facebook_business_adset.AdSet.Field.status,] )
    active_adset_id_list = [adset_object.get("id") for adset_object in campaign_object if adset_object.get("status")=='ACTIVE']
    adset = facebook_business_adset.AdSet(int(active_adset_id_list[0]))
    adset_object = adset.remote_read( fields=['promoted_object'] )
    promoted_object = adset_object.get("promoted_object")
    pixel_id = promoted_object.get("pixel_id") if promoted_object else None
    if pixel_id is None:
        print('[facebook_lookalike_audience.save_campaign_pixel_id]: No pixel id in this campaign')
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


# In[12]:


def is_custom_audience_created(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = "SELECT * FROM campaign_pixel_id WHERE campaign_id={} AND is_created='True'".format(campaign_id)
    my_cursor.execute(sql)
    result = my_cursor.fetchall()
    my_db.commit()
    my_cursor.close()
    my_db.close()
    return len(result) == len(CONVERSION_BEHAVIOR_LIST)

def is_lookalike_audience_created(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    # First check campaign in db
    sql = "SELECT * FROM campaign_pixel_id WHERE campaign_id={}".format(campaign_id)
    my_cursor.execute(sql)
    result = my_cursor.fetchall()
    if len(result) == 0:
        print('[is_lookalike_audience_created]: campaign not in DB.')
        save_campaign_pixel_id(campaign_id)
        return False
    # Then check campaign lookalike in adset
    lookalike_sql = "SELECT * FROM campaign_pixel_id WHERE campaign_id={} AND is_created='True'".format(campaign_id)
    my_cursor.execute(lookalike_sql)
    result = my_cursor.fetchall()
    my_db.commit()
    my_cursor.close()
    my_db.close()
    return len(result) == len(CONVERSION_BEHAVIOR_LIST)

def is_operation_status_normal(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    # First check campaign in db
    sql = "SELECT * FROM campaign_pixel_id WHERE campaign_id={}".format(campaign_id)
    my_cursor.execute(sql)
    result = my_cursor.fetchall()
    if len(result) == 0:
        print('[is_operation_status_normal]: campaign not in DB.')
        save_campaign_pixel_id(campaign_id)
        return False
    # Then check custom audience operation status
    operation_status_sql = "SELECT * FROM campaign_pixel_id WHERE campaign_id={} AND operation_status='Normal'".format(campaign_id)
    my_cursor.execute(operation_status_sql)
    result = my_cursor.fetchall()
    my_db.commit()
    my_cursor.close()
    my_db.close()
    return len(result) > 0


# In[13]:


def retrieve_custom_audience_spec(campaign_id):
    audience_attribute_list = []
    custom_audience_dict = get_custom_audience_id(campaign_id)
    for audience_id in custom_audience_dict.values():
        custom_audience = facebook_business_custom_audience.CustomAudience(audience_id)
        audience_attribute = custom_audience.remote_read(fields=[
            custom_audience.Field.data_source,
            custom_audience.Field.operation_status,
            custom_audience.Field.retention_days,
            custom_audience.Field.approximate_count,
        ])
        approximate_count = audience_attribute.get("approximate_count")
        operation_status = audience_attribute.get("operation_status").get("description") if audience_attribute.get("operation_status").get("code")==200 else "Abnormal"
        retention_days = audience_attribute.get("retention_days")
        data_source = audience_attribute.get("data_source")
        audience_id = audience_attribute.get("id")
        audience_attribute = {
            "audience_id": audience_id,
            "retention_days": retention_days,
            "operation_status": operation_status, 
            "approximate_count": approximate_count,
            "data_source": data_source
        }
        audience_attribute_list.append(audience_attribute)
    return audience_attribute_list

def update_audience_attribute(audience_id, retention_days, operation_status, approximate_count, data_source):    
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    update_sql = ("UPDATE campaign_pixel_id SET data_source='{}', retention_days={}, operation_status='{}', approximate_count={} WHERE audience_id={}".format( data_source, retention_days, operation_status, approximate_count, audience_id ) )
    my_cursor.execute(update_sql)
    my_db.commit()
    my_cursor.close()
    my_db.close()
    return


# In[14]:


def process_campaign_custom_audience(campaign_id):
    if is_lookalike_audience_created(campaign_id):
        print('[process_campaign_custom_audience]: lookalike audience is created.')
        return
    else:
        print('[process_campaign_custom_audience] lookalike audience not created.')
        create_campaign_custom_audience_by_pixel(campaign_id)


# In[ ]:


def save_pixel_id_for_one_campaign(campaign_id):
    save_campaign_pixel_id(campaign_id)
    process_campaign_custom_audience(campaign_id)


# In[15]:


def save_pixel_id_for_all_campaign():
    performance_campaign_list = mysql_saver.get_running_performance_campaign().to_dict('records')
    
    for campaign in performance_campaign_list:
        account_id = campaign.get("account_id")
        campaign_id = campaign.get("campaign_id")
        permission.init_facebook_api(account_id)
        
        print('[save_pixel_id_for_all_campaign] conversion campaign_id:', campaign_id)
        save_pixel_id_for_one_campaign(campaign_id)
    


# In[16]:


def update_all_custom_audience():
    conversion_campaign_list = mysql_saver.get_running_performance_campaign().to_dict('records')
    print('[update_all_custom_audience]: conversion_campaign_id_list')
    print(conversion_campaign_list)
    for campaign in conversion_campaign_list:
        account_id = campaign.get("account_id")
        campaign_id = campaign.get("campaign_id")
        permission.init_facebook_api(account_id)
        audience_attribute_list = retrieve_custom_audience_spec(campaign_id)
        for audience_attribute in audience_attribute_list:
            update_audience_attribute(**audience_attribute)


# In[17]:


def main():
    save_pixel_id_for_all_campaign()
    update_all_custom_audience()


# In[18]:


if __name__ == "__main__":
    main()


# In[52]:


#!jupyter nbconvert --to script facebook_lookalike_audience.ipynb


# In[20]:


# def make_lookalike_adset(campaign_id):
#     import facebook_externals as externals
#     import facebook_datacollector as collector
    
# #     df = mysql_saver.get_campaign_target(campaign_id)
    
#     # Testing
#     mydb = mysql_saver.connectDB(mysql_saver.DATABASE)
#     request_date = datetime.datetime.now().date()
#     df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id='{}' AND ai_status='active'".format(campaign_id), con=mydb )
#     mydb.close()
    
    
    
#     charge_type = df['charge_type'].iloc[0]
#     campaign_instance = collector.Campaigns(campaign_id, charge_type)
#     adsets_active_list = campaign_instance.get_adsets_active()
#     adset_params = externals.retrieve_origin_adset_params(adsets_active_list[0])
#     adset_params.pop("id")
#     ad_id_list = externals.get_ad_id_list(adsets_active_list[0])

#     targeting = adset_params.get("targeting")
#     targeting.pop("flexible_spec", None)
    
#     if is_lookalike_audience_created(campaign_id):
#         return
#     save_pixel_id_for_all_campaign(campaign_id)
#     lookalike_audience_dict = get_lookalike_audience_id(campaign_id)

#     if not any(lookalike_audience_dict):
#         print('[make_lookalike_adset]: all lookalike is in adset.')
#         return
#     for lookalike_behavior in lookalike_audience_dict.keys():
#         lookalike_audience_id = lookalike_audience_dict[lookalike_behavior]
#         targeting["custom_audiences"] = [{"id": lookalike_audience_id}]
#         adset_params["name"] = "Look-a-like Custom {}".format(lookalike_behavior)
#         print('==================')
#         print(adset_params)

#     #     try:
#         new_adset_id = externals.make_adset(adset_params)
#         print('[copy_adset] make_adset success, adset_id', adsets_active_list[0], ' new_adset_id', new_adset_id)
#         time.sleep(10)

#         for ad_id in ad_id_list:
#             result_message = externals.assign_copied_ad_to_new_adset(new_adset_id=new_adset_id, ad_id=ad_id)
#             print('[copy_adset] result_message', result_message)
#         modify_result_db(campaign_id, lookalike_audience_id, True)


# In[ ]:




