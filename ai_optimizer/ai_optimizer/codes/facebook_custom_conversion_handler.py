#!/usr/bin/env python
# coding: utf-8

# In[6]:



from pathlib import Path
import datetime
import time

from facebook_business.api import FacebookAdsApi
import facebook_business.adobjects.adaccount as facebook_business_adaccount
import facebook_business.adobjects.adset as facebook_business_adset
import facebook_business.adobjects.campaign as facebook_business_campaign
import facebook_business.adobjects.adsinsights as facebook_business_adsinsights
import facebook_business.adobjects.adaccounttargetingunified as facebook_business_adaccounttarget
import facebook_business.adobjects.customconversion as facebook_business_customconversion

import facebook_datacollector as fb_collector
import mysql_adactivity_save as mysql_saver
import adgeek_permission as permission


# In[7]:



def get_account_id_by_campaign(campaign_id):
    this_campaign = facebook_business_campaign.Campaign( campaign_id ).remote_read(fields=["account_id"])
    account_id = this_campaign.get('account_id')
    return account_id
    


# In[8]:


def save_account_custom_conversions_intodb(account_id, campaign_id, customconversions_id_list):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    
    for customconversions_id in customconversions_id_list:
        this_customconversions = facebook_business_customconversion.CustomConversion(customconversions_id)
        customconversions_result = this_customconversions.remote_read(fields=["name", 'rule' , 'id'])
        conversion_name = this_customconversions.get('name')
        conversion_rule = this_customconversions.get('rule')
        conversion_id  = this_customconversions.get('id')
        print('[save_custom_conversions_intodb] conversion_id:', conversion_id, conversion_name, conversion_rule)
        
        sql = "INSERT INTO facebook_custom_conversion ( account_id, campaign_id, conversion_id, conversion_name, conversion_rule, created_at ) VALUES ( %s, %s, %s, %s, %s, %s )"
        val = ( int(account_id), int(campaign_id), int(conversion_id), conversion_name, conversion_rule, datetime.datetime.now() )
        my_cursor.execute(sql, val)
        my_db.commit()  
      
    my_cursor.close()
    my_db.close()


# In[9]:


def get_account_custom_conversions(account_id, campaign_id):
    account_id_act = 'act_' + str(account_id)
    this_account = facebook_business_adaccount.AdAccount(account_id_act)
    customconversions_result = this_account.remote_read(fields=["customconversions"])
#     print('[process_account_custom_conversion] customconversions_result:', customconversions_result)
    
    customconversions_id_list = []
    if customconversions_result and customconversions_result.get('customconversions'):
        for result in customconversions_result.get('customconversions').get('data'):
            customconversions_id_list.append( int(result.get('id')))
#     print('[process_account_custom_conversion] customconversions_id_list:', customconversions_id_list)
    
    save_account_custom_conversions_intodb(account_id, campaign_id, customconversions_id_list)
      
    
    
    


# In[10]:


def process_account_custom_conversion(campaign_id):
    account_id = get_account_id_by_campaign(campaign_id)
    print('[process_account_custom_conversion] account_id:', account_id, 'campaign_id:', campaign_id)
    
    get_account_custom_conversions(account_id, campaign_id)
    
    
    


# In[11]:


def save_adset_optimization_goal_intodb(campaign_id, adset_id_list):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    
    for adset_id in adset_id_list:
        this_adset = facebook_business_adset.AdSet( adset_id ).remote_read(fields=['name','id','optimization_goal','optimization_sub_event','promoted_object'])
        print('[save_adset_optimization_goal_intodb]', this_adset)
        adset_name = this_adset.get('name')
        
        promoted_object = this_adset.get('promoted_object')
        pixel_rule = None
        pixel_id = 0
        custom_event_type = ''
        if promoted_object:
            pixel_rule = promoted_object.get('pixel_rule')
            pixel_id = promoted_object.get('pixel_id')
            custom_event_type = promoted_object.get('custom_event_type')
        print('[save_adset_optimization_goal_intodb]', campaign_id ,adset_id ,adset_name ,pixel_rule ,pixel_id ,custom_event_type)
        
        if pixel_rule:
            sql = "INSERT INTO facebook_adset_optimization_goal ( campaign_id, adset_id, adset_name, pixel_rule, pixel_id, custom_event_type, created_at ) VALUES ( %s, %s, %s, %s, %s, %s, %s )"
            val = ( int(campaign_id), int(adset_id), adset_name, pixel_rule, int(pixel_id), custom_event_type, datetime.datetime.now()  )
            my_cursor.execute(sql, val)
            my_db.commit()  
      
            
    my_cursor.close()
    my_db.close()    
    


# In[12]:


def process_campaign_adset_optimization_goal(campaign_id):
    this_campaign = facebook_business_campaign.Campaign(campaign_id)
    adset_ids = this_campaign.get_ad_sets(fields = [ facebook_business_adset.AdSet.Field.id ])
    
    adset_id_list = []
    for adset_id in adset_ids:
        adset_id_list.append( int(adset_id.get('id')))
    print('[process_campaign_adset_optimization_goal] adset_id_list', adset_id_list)
    save_adset_optimization_goal_intodb(campaign_id, adset_id_list)
    


# In[21]:


def get_conversion_id_by_rule(pixel_rule, campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = "SELECT conversion_id FROM facebook_custom_conversion where conversion_rule = %(conversion_rule)s and campaign_id = %(campaign_id)s"
    val = {'conversion_rule': pixel_rule, 'campaign_id': str(campaign_id) }
    my_cursor.execute(sql,val)
    result = my_cursor.fetchall()
    my_db.commit()
    my_cursor.close()
    my_db.close()

    if len(result) > 0:
        first_row_tupe = result[0]
        conversion_id = first_row_tupe[0]
        print('[get_rule_by_adset_id] conversion_id:',conversion_id)
        return conversion_id
            
            
    return None
      
    


# In[9]:


def get_rule_by_adset_id(adset_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = 'SELECT pixel_rule FROM facebook_adset_optimization_goal where adset_id = {}  order by `created_at` DESC'.format(adset_id)
    my_cursor.execute(sql)
    result = my_cursor.fetchall()
    my_db.commit()
    my_cursor.close()
    my_db.close()
    
    if len(result) > 0:
        first_row_tupe = result[0]
        pixel_rule = first_row_tupe[0]
        print('[get_rule_by_adset_id] pixel_rule:',pixel_rule)
        return pixel_rule
            
    return None
      
    


# In[10]:


def get_campaign_custom_goal_id(campaign_id):
    this_campaign = facebook_business_campaign.Campaign(campaign_id)
    adset_ids = this_campaign.get_ad_sets(fields = [ facebook_business_adset.AdSet.Field.id ])
    
    adset_id_list = []
    for adset_id in adset_ids:
        adset_id_list.append( int(adset_id.get('id')))
    
    for adset_id in adset_id_list:
#         print('----')
        pixel_rule = get_rule_by_adset_id(adset_id)
        if pixel_rule:
            conversion_id = get_conversion_id_by_rule(pixel_rule, campaign_id)
#             print('conversion_id:', conversion_id)
#             print('pixel_rule:', pixel_rule)
            if conversion_id:
                return conversion_id
#         print('===')
            
    return None
    
            
        
    


# In[11]:


def get_conversion_id_by_compaign(campaign_id):
    conversion_id = get_campaign_custom_goal_id(campaign_id)

    if conversion_id:
        print('[get_conversion_id_by_compaign] conversion_id already existed:',conversion_id)
    else:
        process_account_custom_conversion(campaign_id)
        process_campaign_adset_optimization_goal(campaign_id)
        conversion_id = get_campaign_custom_goal_id(campaign_id)
        print('[get_conversion_id_by_compaign] conversion_id:',conversion_id)
    return conversion_id


# In[12]:


def main():
    campaign_id = 23843417575950621
    conversion_id = get_conversion_id_by_compaign(campaign_id)
    print('main', conversion_id)
    


# In[13]:



    
if __name__ == "__main__":
    main()
    


# In[ ]:




