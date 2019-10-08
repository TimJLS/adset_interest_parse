#!/usr/bin/env python
# coding: utf-8

# In[1]:


import facebook_business.adobjects.adset as facebook_business_adset
import facebook_business.adobjects.campaign as facebook_business_campaign
import facebook_business.adobjects.adaccount as facebook_business_adaccount
import facebook_business.adobjects.customaudience as facebook_business_custom_audience

import facebook_datacollector as collector
import database_controller
import adgeek_permission as permission

import pandas as pd
import datetime
import time



CONVERSION_BEHAVIOR_LIST = ['Purchase', 'AddToCart', 'ViewContent']


# In[2]:


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


# In[3]:


def modify_result_db(campaign_id, lookalike_audience_id, is_lookalike_in_adset):
    #get date
    opt_date = datetime.datetime.now()
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    database_fb.upsert(
        "campaign_pixel_id",
        {
            'is_lookalike_in_adset': is_lookalike_in_adset,
            'updated_at': opt_date,
            'campaign_id': campaign_id,
            'lookalike_audience_id': lookalike_audience_id,
        }
    )
    database_fb.dispose()


# In[4]:


def get_custom_audience_id(campaign_id):
    custom_audience_dict = dict()
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    df_pixel_id = database_fb.retrieve("campaign_pixel_id", campaign_id=campaign_id, by_request_time=False)
    df_pixel_id = df_pixel_id[df_pixel_id.is_created=='True'][['behavior_type', 'audience_id']]
    database_fb.dispose()
    if df_pixel_id.empty:
        print('[get_custom_audience_id]: No custom audience is created.')
        return 
    for idx, row in df_pixel_id.iterrows():
        custom_audience_dict[row['behavior_type']] = row['audience_id']

    return custom_audience_dict


# In[5]:


def get_lookalike_audience_id(campaign_id):
    import collections
    lookalike_audience_dict = collections.OrderedDict()
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    df_pixel_id = database_fb.retrieve("campaign_pixel_id", campaign_id=campaign_id, by_request_time=False)
    df_is_in_adset = df_pixel_id[df_pixel_id.is_lookalike_in_adset=='False'].sort_values(by=['approximate_count'], ascending=False)
    df_is_in_adset = df_is_in_adset[df_is_in_adset.approximate_count>1000]
    database_fb.dispose()
    if df_pixel_id.empty:
        print('[get_lookalike_audience_id]: No lookalike audience is created.')
    elif df_is_in_adset.empty:
        print('[get_lookalike_audience_id]: All lookalike audience is in adset.')
    
    for idx, row in df_is_in_adset.iterrows():
        lookalike_audience_dict[row['behavior_type']] = row['lookalike_audience_id']
    return lookalike_audience_dict


# In[6]:


def is_lookalike_created(campaign_id):
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    df = database_fb.retrieve("campaign_pixel_id", campaign_id, by_request_time=False)
    database_fb.dispose()
    return not df.empty


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
        return
    lookalike_audience_id = resp.get("id")
    print('==========[lookalike response]')
    print(resp)
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    database_fb.upsert(
        "campaign_pixel_id",
        {
            'is_created': 'True',
            'lookalike_audience_id': lookalike_audience_id,
            'campaign_id': campaign_id,
            'behavior_type': behavior_type,
        },
    )
    database_fb.dispose()
    return


# In[8]:


def create_campaign_custom_audience_by_pixel(campaign_id):
    campaign = facebook_business_campaign.Campaign(campaign_id)
    campaign_object = campaign.get_insights(
        fields = [facebook_business_campaign.Campaign.Field.account_id] )
    account_id = campaign_object[0].get("account_id")    
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    df = database_fb.retrieve("campaign_pixel_id", campaign_id=campaign_id, by_request_time=False)
    df_not_opted_pixel_id = df[df.is_created=='False']
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
                return
            print('==================[custom]')
            print(resp)
            audience_id = resp.get("id")
            
            create_lookalike_custom_audience(account_id, campaign_id, behavior_type, audience_id)
            
            update_sql = ("UPDATE campaign_pixel_id SET is_created='{}', audience_id={} WHERE campaign_id={} AND behavior_type='{}'".format( True, audience_id, campaign_id, behavior_type ) )
            database_fb.upsert("campaign_pixel_id", {
                "is_created": "True",
                "audience_id": audience_id,
                "campaign_id": campaign_id,
                "behavior_type": behavior_type,
            })
    database_fb.dispose()
    return


# In[9]:


def get_not_processed_lookalike_df(campaign_id):
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    df_saved_pixel_id = database_fb.retrieve("campaign_pixel_id", campaign_id=campaign_id, by_request_time=False)
    df_saved_pixel_id = df_saved_pixel_id[df_saved_pixel_id.is_lookalike_in_adset=='False']
    database_fb.dispose()
    return df_saved_pixel_id


# In[10]:


def get_processed_lookalike_df(campaign_id):
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    df_saved_pixel_id = database_fb.retrieve("campaign_pixel_id", campaign_id=campaign_id, by_request_time=False)
    df_saved_pixel_id = df_saved_pixel_id[df_saved_pixel_id.is_lookalike_in_adset=='True']
    database_fb.dispose()
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

    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    for behavior_type in CONVERSION_BEHAVIOR_LIST:
        
        database_fb.insert_ignore(
            "campaign_pixel_id",
            {
                'campaign_id': campaign_id,
                'behavior_type': behavior_type,
                'pixel_id': pixel_id,
                'is_created': 'False',
            }
        )
    database_fb.dispose()
    return


# In[12]:


def is_custom_audience_created(campaign_id):
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    df_pixel_id = database_fb.retrieve("campaign_pixel_id", campaign_id=campaign_id, by_request_time=False)
    df_pixel_id = df_pixel_id[df_pixel_id.is_created=='True']
    database_fb.dispose()
    return len(df_pixel_id.index) == len(CONVERSION_BEHAVIOR_LIST)


# In[13]:


def is_lookalike_audience_created(campaign_id):
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    df_pixel_id = database_fb.retrieve("campaign_pixel_id", campaign_id=campaign_id, by_request_time=False)
    database_fb.dispose()
    if df_pixel_id.empty:
        print('[is_lookalike_audience_created]: campaign not in DB.')
        save_campaign_pixel_id(campaign_id)
        return False
    df_lookalike = df_pixel_id[df_pixel_id.is_created=='True']
    return len(df_lookalike.index) == len(CONVERSION_BEHAVIOR_LIST)


# In[14]:


def is_operation_status_normal(campaign_id):
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    df_pixel_id = database_fb.retrieve("campaign_pixel_id", campaign_id=campaign_id, by_request_time=False)
    database_fb.dispose()
    if df_pixel_id.empty:
        print('[is_operation_status_normal]: campaign not in DB.')
        save_campaign_pixel_id(campaign_id)
        return False
    df_operation = df_pixel_id[df_pixel_id.operation_status=='Normal']
    return df_operation.empty


<<<<<<< HEAD
# In[13]:
=======
# In[15]:
>>>>>>> 9d00ebbdcedbfed6cee913fd16ed3e9bcc22e9ed


def retrieve_custom_audience_spec(campaign_id):
    audience_attribute_list = []
    custom_audience_dict = get_custom_audience_id(campaign_id)
    if bool(custom_audience_dict):
        for audience_id in custom_audience_dict.values():
            if audience_id:
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


# In[16]:


def update_audience_attribute(audience_id, retention_days, operation_status, approximate_count, data_source):
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    
    database_fb.update(
        "campaign_pixel_id",
        {
            'retention_days': retention_days,
            'operation_status': operation_status,
            'approximate_count': approximate_count,
            'data_source': str(data_source),
        },
        audience_id=audience_id,
    )
    database_fb.dispose()
    return


<<<<<<< HEAD
# In[14]:
=======
# In[17]:
>>>>>>> 9d00ebbdcedbfed6cee913fd16ed3e9bcc22e9ed


def process_campaign_custom_audience(campaign_id):
    if is_lookalike_audience_created(campaign_id):
        print('[process_campaign_custom_audience]: lookalike audience is created.')
        return
    else:
        print('[process_campaign_custom_audience] lookalike audience not created.')
        create_campaign_custom_audience_by_pixel(campaign_id)


<<<<<<< HEAD
# In[15]:
=======
# In[18]:
>>>>>>> 9d00ebbdcedbfed6cee913fd16ed3e9bcc22e9ed


def save_pixel_id_for_one_campaign(campaign_id):
    save_campaign_pixel_id(campaign_id)
    process_campaign_custom_audience(campaign_id)


<<<<<<< HEAD
# In[16]:
=======
# In[19]:
>>>>>>> 9d00ebbdcedbfed6cee913fd16ed3e9bcc22e9ed


def save_pixel_id_for_all_campaign():
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    performance_campaign_list = database_fb.get_performance_campaign().to_dict('records')
    database_fb.dispose()
    for campaign in performance_campaign_list:
        account_id = campaign.get("account_id")
        campaign_id = campaign.get("campaign_id")
        permission.init_facebook_api(account_id)
        
        print('[save_pixel_id_for_all_campaign] conversion campaign_id:', campaign_id)
        save_pixel_id_for_one_campaign(campaign_id)
    


<<<<<<< HEAD
# In[17]:
=======
# In[20]:
>>>>>>> 9d00ebbdcedbfed6cee913fd16ed3e9bcc22e9ed


def update_all_custom_audience():
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    conversion_campaign_list = database_fb.get_performance_campaign().to_dict('records')
    database_fb.dispose()
    print('[update_all_custom_audience]: conversion_campaign_id_list')
    print(conversion_campaign_list)
    for campaign in conversion_campaign_list:
        account_id = campaign.get("account_id")
        campaign_id = campaign.get("campaign_id")
        permission.init_facebook_api(account_id)
        audience_attribute_list = retrieve_custom_audience_spec(campaign_id)
        for audience_attribute in audience_attribute_list:
            update_audience_attribute(**audience_attribute)


# In[21]:


def main():
    save_pixel_id_for_all_campaign()
    update_all_custom_audience()


# In[24]:


if __name__ == "__main__":
    main()


<<<<<<< HEAD
# In[20]:
=======
# In[23]:
>>>>>>> 9d00ebbdcedbfed6cee913fd16ed3e9bcc22e9ed


# !jupyter nbconvert --to script facebook_lookalike_audience.ipynb


# In[ ]:




