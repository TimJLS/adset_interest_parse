#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gsn_db
import datetime
from googleads import adwords
import pandas as pd
import copy
import math
import datetime
from enum import Enum
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)


# In[2]:


class Status(object):
    enable = 'ENABLED'
    pause = 'PAUSED'


# In[3]:


class OperatorContainer():
    def __init__(self):
        self.selector = [{
            'fields': None,
            'predicates': [{
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values':[None]
            }]
        }]
        self.selector_ad = [{
            'fields': None,
            'predicates': [
                {
                    'field': 'Id',
                    'operator': 'EQUALS',
                    'values':[None]
                },
                {
                    'field': 'AdGroupId',
                    'operator': 'EQUALS',
                    'values':[None]
                }
            ]
        }]
        self.selector_campaign = [{
            'fields': None,
            'predicates': [{
                'field': 'CampaignId',
                'operator': 'EQUALS',
                'values':[None]
            },
            {
                'field': 'Status',
                'operator': 'EQUALS',
                'values':'ENABLED'
            }]
        }]
        self.criterion = {
            'id':None, 
            'xsi_type':None,
        }
        self.operand = {
            'id': None,
        }
        self.criterion_operand = {
            'adGroupId': None,
            'xsi_type': None,
            'criterion': None,
        }
        self.operation = {
            'operator': None,
            'operand': None
        }
        self.operations = []


# In[ ]:




