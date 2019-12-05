#!/usr/bin/env python
# coding: utf-8

# In[3]:


import requests
import json
import database_controller


# In[11]:


ACCOUNT_URL = "https://mpc.adgeek.net/v2/accounts?"     #filter[account_id]=6242788100&filter[media]=gdn


# In[18]:


def get_currency(campaign_id, media):
    if media.lower() == 'gdn':
        database = database_controller.GDN(database_controller.Database)
    elif media.lower() == 'gsn':
        database = database_controller.GSN(database_controller.Database)
    else:
        raise ValueError("the parameter 'media' should be 'gdn' or 'gsn'")
    
    campaign = database.get_one_campaign(campaign_id).to_dict('records')
    currency_url = ACCOUNT_URL + "filter[account_id]={}&filter[media]={}".format(campaign_id, media.lower())
    
    resp = requests.get(currency_url)
    if resp.status_code == requests.codes.ok:
        content = json.loads(resp.text)
        return content.get('currency_code', None)


# In[ ]:




