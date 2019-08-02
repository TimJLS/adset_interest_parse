#!/usr/bin/env python
# coding: utf-8

# In[1]:


import facebook_business.adobjects.adset as facebook_business_adset
import facebook_business.adobjects.campaign as facebook_business_campaign
import facebook_business.adobjects.adaccount as facebook_business_adaccount
import facebook_business.adobjects.customaudience as facebook_business_custom_audience

from facebook_business.api import FacebookAdsApi
import facebook_datacollector as collector
import mysql_adactivity_save as mysql_saver

import pandas as pd
my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBALrHTgMWgRujnWcZA3ZB823phs6ynDDtQxnzIZASyRQZCHfr5soXBZA7NM9Dc4j9O8FtnlIzxiPCsYt4tmPQ6ZAT3yJLPuYQqjnWZBWX5dsOVzNhEqsHYj1jVJ3RAVVueW7RSxRDbNXKvK3W23dcAjNMjxIjQGIOgZDZD'

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)


# # Get Custom Audience's attribute

# In[45]:


ca = facebook_business_custom_audience.CustomAudience(23843450947680647).remote_read(fields=[
    "account_id",
    "data_source",
    "delivery_status",
    "description",
    "id",
    "lookalike_audience_ids",
    "lookalike_spec",
    "name",
    "operation_status",
    "opt_out_link",
    "permission_for_actions",
    "pixel_id",
    "retention_days",
    "rule",
    "subtype",
    "time_updated",
    "approximate_count"])


# In[46]:


ca


# # value-based cusotm audience(one pixel in value-based)

# In[ ]:


camp = Campaign(23843346060540647)
adset = AdSet(23843434329230647)
ad_acc = AdAccount('act_171560460354321')
fields = [
]
params = {
  'name': 'Test Value-Based lookalike from Pixel ViewContent',
  'subtype': 'LOOKALIKE',
  'lookalike_spec': {
      'origin_event_sources':[{
          'id':'288842628153699',
          'event_names':['ViewContent']
  }],
  'type':'custom_ratio',
  'ratio':0.01,
  'country':'TW'},
}
print(ad_acc.create_custom_audience(
    fields=fields,
    params=params,
))


# # look-a-like audience
# (need one custom audience id to be origin_audience_id)

# In[ ]:


from facebook_business.adobjects.customaudience import CustomAudience

lookalike = CustomAudience(parent_id='act_171560460354321')
lookalike.update({
    CustomAudience.Field.name: 'My lookalike audience',
    CustomAudience.Field.subtype: CustomAudience.Subtype.lookalike,
    CustomAudience.Field.origin_audience_id: '23843434379220647',
    CustomAudience.Field.lookalike_spec: {
        'type': 'similarity',
        'country': 'TW',
    },
})

lookalike.remote_create()
print(lookalike)


# same as above, except apointing ratios

# In[ ]:


lookalike = CustomAudience(parent_id='act_171560460354321')
lookalike.update({
    CustomAudience.Field.subtype: CustomAudience.Subtype.lookalike,
    CustomAudience.Field.lookalike_spec: {
        'origin_ids': '23843346104340647',
#         'starting_ratio': 0.03,
        'ratio': 0.1,
        'conversion_type': 'campaign_conversions',
        'country': 'TW',
    },
})

lookalike.remote_create()
print(lookalike)


# # offline conversion custom audience by pixel id (WE NEED THE MOST)

# In[ ]:


fields = [
]
params = {
    'name': 'My Test Website Custom Audience',
    "operator" : "and",
    'rule': {
        "inclusions": {
            "operator": "or",
            "rules":[{
                "event_sources": [{'id': 288842628153699, 'type': 'pixel'}], 
                "retention_seconds" : 2592000,
                "filter": {
                    "operator": "or",
                    "filters": [
                        {
                            "field": "event",
                            "operator": "=",
    #                         "value": "offsite_conversion.fb_pixel_view_content",
                            "value": "Purchase",
#                             "value": "ViewContent",
                        }
                    ]
                },
                "aggregation" : {
                    "type":"count",
                    "operator":">",
                    "value":1
                }
            }]
        }
    }
}
resp = AdAccount('act_171560460354321').create_custom_audience(
    fields=fields,
    params=params,
)

