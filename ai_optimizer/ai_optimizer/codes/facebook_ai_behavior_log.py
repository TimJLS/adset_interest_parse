#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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
import database_controller
import facebook_currency_handler as currency_handler


class BehaviorType:
    COPY = 'copy'
    CLOSE = 'close'
    CREATE = 'create'
    ADJUST = 'adjust'


# In[ ]:


def get_adset_name_bidding(adset_id):
    this_adset = facebook_business_adset.AdSet( adset_id).remote_read(fields=["campaign_id", "name", "bid_amount"])
    return this_adset.get('campaign_id'), this_adset.get('name'), this_adset.get('bid_amount')


# In[ ]:


def save_adset_behavior(adset_id, behavior_type, behavior_misc = '' ):
    auto_bidding_string = 'AutoBidding'
    campaign_id, adset_name , adset_bid = get_adset_name_bidding(adset_id)
    created_at = int(time.time())
    
    if behavior_type == BehaviorType.ADJUST:
        if adset_bid == behavior_misc:
            return
        behavior_misc = str(adset_bid) + ':' + str(behavior_misc)
    
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    database_fb.insert(
        "ai_behavior_log",
        {
            'campaign_id': int(campaign_id),
            'adset_id': int(adset_id),
            'adset_name': adset_name,
            'behavior': behavior_type,
            'behavior_misc': behavior_misc if not adset_bid or adset_bid != 0 else auto_bidding_string,
            'created_at': int(created_at),
        }
    )
    


# In[ ]:



if __name__ == "__main__":
    save_adset_behavior(23843602031340098, BehaviorType.ADJUST)
#     print(adset_name , adset_bid )
    
    


# In[ ]:


# !jupyter nbconvert --to script facebook_ai_behavior_log.ipynb

