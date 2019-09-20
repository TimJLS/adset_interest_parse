#!/usr/bin/env python
# coding: utf-8

# In[2]:


import datetime
import gdn_db
from googleads import adwords
import pandas as pd
import copy
import math
import datetime
from enum import Enum
import adgeek_permission as permission


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


# In[20]:


class CampaignServiceContainer(object):

    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.adwords_client = permission.init_google_api(self.customer_id)
        self.service_campaign = adwords_client.GetService('CampaignService', version='v201809')
        self.operator_container = OperatorContainer()
        self.ad_group = AdGroup
        
class Campaign(object):
    fields = ['CampaignId', 'Name', 'AdGroupId']
    def __init__(self, service_container, campaign_id):
        self.campaign_id = campaign_id
        self.service_container = service_container
        self.operator_container = OperatorContainer()
        self.ad_groups = []
        
    def generate_ad_group_id_type_list(self,):
        self.native_ad_group_id_list = []
        self.mutate_ad_group_id_list = []
        self.operator_container.selector_campaign[0]['fields'] = self.fields
        self.operator_container.selector_campaign[0]['predicates'][0]['values'][0] = self.campaign_id
        results = self.service_container.service_ad_group.get(self.operator_container.selector_campaign)['entries']
        for result in results:
            if 'Mutant' in result['name'].split():
                self.mutate_ad_group_id_list.append( result['id'] )                
            else:
                self.native_ad_group_id_list.append( result['id'] )
        return {
            'mutant': self.mutate_ad_group_id_list,
            'native': self.native_ad_group_id_list
        }
    
    def get_ad_groups(self,):
        self.operator_container.selector_campaign[0]['fields'] = self.fields
        self.operator_container.selector_campaign[0]['predicates'][0]['values'][0] = self.campaign_id
        results = self.service_container.service_ad_group.get(self.operator_container.selector_campaign)['entries']
        for result in results:
            ad_group = AdGroup(self.service_container, result['id'])
            self.ad_groups.append(ad_group)
        return results


# In[5]:


class AdGroupServiceContainer(object):

    def __init__(self, customer_id=None):
        if customer_id:
            self.customer_id = customer_id
            self.adwords_client = permission.init_google_api(self.customer_id)
        self.service_ad_group = adwords_client.GetService('AdGroupService', version='v201809')
        self.service_criterion = adwords_client.GetService('AdGroupCriterionService', version='v201809')
        self.service_ad = adwords_client.GetService('AdGroupAdService', version='v201809')
        self.operator_container = OperatorContainer()
        self.ad_group = AdGroup
        
    def set_ad_group(self, ad_group_id):
        self.ad_group = AdGroup(ad_group_id)
        return self.ad_group
    
    def make_ad_group(self, base_ad_group):
        self.datetime = datetime.datetime.now()
        ad_group_params = base_ad_group.param.retrieve()
        campaign_id = ad_group_params[0]['campaignId']
        xsi_type = ad_group_params[0]['biddingStrategyConfiguration']['bids'][0]['Bids.Type']
        bid_micro_amount = ad_group_params[0]['biddingStrategyConfiguration']['bids'][0]['bid']['microAmount']
        self.operator_container.operand.pop('id')
        self.operator_container.operand['campaignId'] = campaign_id
        self.operator_container.operand['status'] = Status.enable
        self.operator_container.operand['name'] = 'Mutant {}'.format(self.datetime)
        self.operator_container.operand['biddingStrategyConfiguration'] = {
            'bids': [{
                'xsi_type': 'CpcBid',
                'bid': {
                    'microAmount': bid_micro_amount,
                }
            }]
        }
        self.operator_container.operation['operator'] = 'ADD'
        self.operator_container.operation['operand'] = self.operator_container.operand
        self.operator_container.operations.append(self.operator_container.operation)
        result = self.service_ad_group.mutate(self.operator_container.operations)

        new_ad_group_id = result['value'][0]['id']
        
        return AdGroup(self, new_ad_group_id)

        
class AdGroup(object):
    
    def __init__(self, service_container, ad_group_id,):
        self.service_container = service_container
        self.operator_container = OperatorContainer()
        self.ad_group_id = ad_group_id
        self.operations = []
        self.creatives = []
        self.param = self.create_param()
        self.criterions = self.create_criterions()
        self.basic_criterions = self.create_basic_criterions()
        self.user_interest_criterions = self.create_user_interest_criterions()
        self.user_list_criterions = self.create_user_list_criterions()

    def create_creative(self,):
        return Creative

    def create_param(self,):
        return Param(self)

    def create_criterions(self,):
        return Criterion(self)

    def create_basic_criterions(self,):
        return BasicCriterion(self)

    def create_user_interest_criterions(self,):
        return UserInterestCriterion(self)

    def create_user_list_criterions(self,):
        return UserListCriterion(self)

    def get_ads(self,):
        self.operator_container.selector[0]['fields'] = Creative.fields
        self.operator_container.selector[0]['predicates'][0]['values'][0] = self.ad_group_id
        results = self.service_container.service_ad.get(self.operator_container.selector)['entries']
        for result in results:
            ad_id = result['ad']['id']
            creative = Creative(self.service_container.service_ad, self.ad_group_id, ad_id)
            self.creatives.append(creative)
        return self.creatives
    
    def judgeAD():
        return
        

class Creative(object):
    fields = [
        'AdGroupId', 'Id', 'HeadlinePart1', 'HeadlinePart2', 'DisplayUrl', 'CreativeFinalUrls', 'Description', 'Url']
#     def __setattr__(self, name, value):
#         print(name, value)
    def __init__(self, service_ad, ad_group_id, ad_id):
        self.service_ad = service_ad
        self.ad_group_id = ad_group_id
        self.operator_container = OperatorContainer()
        self.ad_id = ad_id
        
    def retrieve(self,):
        self.operator_container.selector_ad[0]['fields'] = self.fields
        self.operator_container.selector_ad[0]['predicates'][0]['values'][0] = self.ad_id
        self.operator_container.selector_ad[0]['predicates'][1]['values'][0] = self.ad_group_id
        result = self.service_ad.get(self.operator_container.selector_ad)
        if result['totalNumEntries'] == 0:
            return
        else:
            return result['entries']
        
    def assign(self, ad_group_id):
        self.ad_group_id = ad_group_id
        result = self.retrieve()
        self.operator_container.operations = []
        self.operator_container.operation['operator'] = 'SET' if result else 'ADD'
        self.operator_container.operand.pop('id')
        self.operator_container.operand['adGroupId'] = self.ad_group_id
        self.operator_container.operand['status'] = Status.enable
        self.operator_container.operand['ad'] = {}
        self.operator_container.operand['ad']['id'] = self.ad_id
        self.operator_container.operation['operand'] = self.operator_container.operand
        self.operator_container.operations.append(self.operator_container.operation)
        result = self.service_ad.mutate(self.operator_container.operations)
        return result
        

class Param(object):
    fields = [
        'CampaignId', 'AdGroupId', 'Name', 'CpcBid', 'BiddingStrategyId','BiddingStrategyName',
        'BiddingStrategySource', 'BiddingStrategyType'
    ]
    def __init__(self, AdGroup):
        self.ad_group = AdGroup
        self.operation_container = OperatorContainer()
        
    def retrieve(self,):
        self.operation_container.selector[0]['fields'] = self.fields
        self.operation_container.selector[0]['predicates'][0]['values'][0] = self.ad_group.ad_group_id
        result = self.ad_group.service_container.service_ad_group.get(self.operation_container.selector)
        self.campaign_id = result['entries']
        return result['entries']
    
    def update_bid(self, bid_micro_amount):
        bid_micro_amount = int(bid_micro_amount * 1000000)
        self.operation_container.operand['id'] = self.ad_group.ad_group_id
        self.operation_container.operand['biddingStrategyConfiguration'] = {
            'bids': [{
                'xsi_type': 'CpcBid',
                'bid': {
                    'microAmount': bid_micro_amount,
                }
            }]
        }
        self.operation_container.operation['operator'] = 'SET'
        self.operation_container.operation['operand'] = self.operation_container.operand
        self.operation_container.operations.append(self.operation_container.operation)
        result = self.ad_group.service_container.service_ad_group.mutate(self.operation_container.operations)
        self.operation_container.operations = []
        return result
    
    def update_status(self, status):
        self.operation_container.operand['id'] = self.ad_group.ad_group_id
        self.operation_container.operand['status'] = status
        self.operation_container.operation['operator'] = 'SET'
        self.operation_container.operation['operand'] = self.operation_container.operand
        self.operation_container.operations.append(self.operation_container.operation)
        result = self.ad_group.service_container.service_ad_group.mutate(self.operation_container.operations)
        self.operation_container.operations = []
        return result

        
class Criterion(object):
    fields = [
        'AdGroupId', 'CriteriaType', 'UserInterestId', 'UserInterestName', 'UserListId', 'VerticalId', 'LabelIds',
        'BiddingStrategyType', 'BiddingStrategySource', 'BiddingStrategyId',
    ]
    def __init__(self, AdGroup):
        self.ad_group = AdGroup
        self.operation_container = OperatorContainer()
        self.operation_container.criterion_operand['adGroupId'] = self.ad_group.ad_group_id
        self.operation_container.criterion_operand['xsi_type'] = 'BiddableAdGroupCriterion'
        self.operation_container.criterion_operand['userStatus'] = 'ENABLED'
        self.operation_container.selector[0]['fields'] = self.fields
        self.operation_container.selector[0]['predicates'][0]['values'][0] = self.ad_group.ad_group_id

        self.criterion_type = None
        
    def retrieve(self,):
        entries = self.ad_group.service_container.service_criterion.get(self.operation_container.selector)['entries']
        biddable_criterions = [ entry for i, entry in enumerate(entries) if entry["AdGroupCriterion.Type"] == 'BiddableAdGroupCriterion']
        negative_criterions = [ entry for i, entry in enumerate(entries) if entry["AdGroupCriterion.Type"] == 'NegativeAdGroupCriterion']
        biddable_criterions = [ entry['criterion'] for i, entry in enumerate(biddable_criterions) if entry['criterion']['type']==self.criterion_type or self.criterion_type is None]
        negative_criterions = [ entry['criterion'] for i, entry in enumerate(negative_criterions) if entry['criterion']['type']==self.criterion_type or self.criterion_type is None]
        return biddable_criterions, negative_criterions

    def update(self, criterions, is_delivering=True, is_included=True):
        biddable_criterions, negative_criterions = self.retrieve()
        all_criterions = biddable_criterions + negative_criterions
        criterion_id_list = [ all_criterion['id'] for all_criterion in all_criterions ]
        
        if not is_delivering:
            self.operation_container.criterion_operand['userStatus'] = 'PAUSED'
        if not is_included:
            self.operation_container.criterion_operand.pop('userStatus')
            self.operation_container.criterion_operand['xsi_type'] =  'NegativeAdGroupCriterion'
        for criterion in criterions:
            self.operation_container.criterion = criterions['id']
            self.operation_container.criterion_operand['adGroupId'] = self.ad_group.ad_group_id
            self.operation_container.criterion_operand['criterion'] = criterions
            self.operation_container.operation['operand'] = self.operation_container.criterion_operand
            self.operation_container.operation['operator'] = 'SET' if  criterions['id'] in criterion_id_list else 'ADD'
            self.operation_container.operations.append(copy.deepcopy(self.operation_container.operation))
        if len(self.operation_container.operations)!=0:
            result = self.ad_group.service_container.service_criterion.mutate(self.operation_container.operations)
            self.operation_container.operations=[]
            return result


class BasicCriterion(Criterion):
    def __init__(self, AdGroup):
        super().__init__(AdGroup)
        self.operation_container.criterion['xsi_type'] = 'AgeRange'
        self.criterion_type = 'AGE_RANGE'


class UserInterestCriterion(Criterion):
    def __init__(self, AdGroup):
        super().__init__(AdGroup)
        self.operation_container.criterion['xsi_type'] = 'CriterionUserInterest'
        self.criterion_type = 'USER_INTEREST'

class UserListCriterion(Criterion):
    def __init__(self, AdGroup):
        super().__init__(AdGroup)
        self.operation_container.criterion['xsi_type'] = 'CriterionUserList'
        self.criterion_type = 'USER_LIST'
    
    def make(self, criterion_id):
        biddable_criterions, negative_criterions = self.retrieve()
        all_criterions = biddable_criterions + negative_criterions
        criterion_id_list = [ all_criterion['id'] for all_criterion in all_criterions ]
        # Make criterion
        self.operation_container.criterion = {'userListId':None, 'xsi_type':None,}
        self.operation_container.criterion['userListId'] = criterion_id
        # Put operations > operand > criterion together
        self.operation_container.operand['criterion'] = self.operation_container.criterion
        self.operation_container.operation['operand'] = self.operation_container.operand
        self.operation_container.operation['operator'] = 'SET' if  criterion_id in criterion_id_list else 'ADD'
            
        self.operation_container.operations.append( copy.deepcopy( self.operation_container.operation ) )
        result = self.ad_group.service_container.service_criterion.mutate(self.operation_container.operations)
        self.operation_container.operations=[]
        return result


# In[6]:


# group_service = AdGroupServiceContainer(customer_id=6714857152,)

# ad_group = AdGroup(group_service, ad_group_id=78188491594)
# ad_group_params = ad_group.param.retrieve()

# params = ad_group.param.retrieve()
# new_ad_group.user_interest_criterions.update(biddable, is_delivering=True, is_included=True)

# creative = Creative(group_service.service_ad, 78188491594, 372422812054 )

# campaign = Campaign(group_service, campaign_id=2053556135)
# campaign.generate_ad_group_id_type_list()


# In[30]:


# campaign_service = CampaignServiceContainer(customer_id=6714857152)
# campaign = Campaign(group_service, campaign_id=2053556135)
# campaign.get_ad_groups()


# In[3]:


#!jupyter nbconvert --to script gdn_controller.ipynb


# In[ ]:




