#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
import time


from facebook_business.api import FacebookAdsApi
# from facebook_business.adobjects.adaccount import AdAccount
import facebook_business.adobjects.adset as facebook_business_adset
# from facebook_business.adobjects.ad import Ad
import facebook_business.adobjects.campaign as facebook_business_campaign
# from facebook_business.adobjects.adcreative import AdCreative
# from facebook_business.adobjects.adactivity import AdActivity
# from facebook_business.adobjects.insightsresult import InsightsResult
# import facebook_business.adobjects.adsinsights as facebook_business_adsinsights

import mysql_adactivity_save as mysql_saver
import facebook_currency_handler as currency_handler


class BehaviorType:
    COPY = 'copy'
    CLOSE = 'close'
    CREATE = 'create'
    ADJUST = 'adjust'


# In[2]:


def get_adset_name_bidding(adset_id):
    this_adset = facebook_business_adset.AdSet( adset_id).remote_read(fields=["campaign_id", "name", "bid_amount"])
    return this_adset.get('campaign_id'), this_adset.get('name'), this_adset.get('bid_amount')


# In[3]:


def save_adset_behavior(adset_id, behavior_type, behavior_misc = '' ):
    campaign_id, adset_name , adset_bid = get_adset_name_bidding(adset_id)
    created_at = int(time.time())
    
    if behavior_type == BehaviorType.ADJUST:
        if adset_bid == behavior_misc:
            return
        behavior_misc = str(adset_bid) + ':' + str(behavior_misc)
    
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = "INSERT  INTO ai_behavior_log ( campaign_id, adset_id, adset_name, behavior, behavior_misc, created_at ) VALUES ( %s, %s, %s, %s, %s, %s )"
    val = ( int(campaign_id), int(adset_id), adset_name, behavior_type, behavior_misc, int(created_at) )
    my_cursor.execute(sql, val)
    my_db.commit()    
    my_cursor.close()
    my_db.close()
    


# In[4]:



if __name__ == "__main__":
    save_adset_behavior(23843602031340098, BehaviorType.ADJUST)
#     print(adset_name , adset_bid )
    
    


# In[8]:


#!jupyter nbconvert --to script facebook_ai_behavior_log.ipynb


# In[ ]:




