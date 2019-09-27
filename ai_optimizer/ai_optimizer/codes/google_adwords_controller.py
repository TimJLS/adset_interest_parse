#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gsn_db
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
    
class Device(object):
    Desktop = 30000
    HighEndMobile = 30001
    ConnectedTv = 30004
    Tablet = 30002


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
        self.selector_bid_modifier = {
            'fields': None,
            'predicates': [{
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values': [None]
            }]
        }
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
        self.selector_budget = [{
            'fields': 'Amount',
            'predicates': [{
                'field': 'CampaignId',
                'operator': 'EQUALS',
                'values': None
            }]
        }]


# In[4]:


class CampaignServiceContainer(object):

    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.adwords_client = permission.init_google_api(account_id=self.customer_id)
        self.service_campaign = self.adwords_client.GetService('CampaignService', version='v201809')
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
        self.ad_groups = [ AdGroup(self.service_container, result['id']) for result in results ]
        return self.ad_groups
    
    def get_keywords(self,):
        if len(self.ad_groups) == 0:
            self.get_ad_groups()
        self.keywords = [ keywords.retrieve() for ad_group in self.ad_groups for keywords in ad_group.get_keywords() ]
        return self.keywords
    
    def get_budget(self,):
        self.operator_container.selector_budget[0]['predicates'][0]['values'] = self.campaign_id
        ad_params = self.service_container.service_campaign.get(self.operator_container.selector_budget)
        if 'entries' in ad_params:
    #         print('ad_params', ad_params)
            for ad_dic in ad_params['entries']:
                if 'budget' in ad_dic and 'amount' in ad_dic['budget'] and 'microAmount' in ad_dic['budget']['amount']:
                    microAmount = ad_dic['budget']['amount']['microAmount']
                    self.amount = microAmount/ 1000000
                    return self.amount


# In[29]:


class AdGroupServiceContainer(object):

    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.adwords_client = permission.init_google_api(account_id=self.customer_id)
        self.service_ad_group = self.adwords_client.GetService('AdGroupService', version='v201809')
        self.service_criterion = self.adwords_client.GetService('AdGroupCriterionService', version='v201809')
        self.service_ad = self.adwords_client.GetService('AdGroupAdService', version='v201809')
        self.service_bid_modifier = self.adwords_client.GetService( 'AdGroupBidModifierService', version='v201809')
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
        self.keywords = []
        self.keyword = self.create_keyword()
        self.param = self.create_param()
        self.criterions = self.create_criterions()
        self.basic_criterions = self.create_basic_criterions()
        self.user_vertical_criterions = self.create_vertical_criterions()
        self.user_interest_criterions = self.create_user_interest_criterions()
        self.user_list_criterions = self.create_user_list_criterions()
        self.bid_modifier = self.create_bid_modifier()
    
    def create_keyword(self,):
        return Keyword
    
    def create_creative(self,):
        return Creative
    
    def create_param(self,):
        return Param(self)
    
    def create_criterions(self,):
        return Criterion(self)
    
    def create_basic_criterions(self,):
        return BasicCriterion(self)
    
    def create_vertical_criterions(self,):
        return UserVerticalCriterion(self)
    
    def create_user_interest_criterions(self,):
        return UserInterestCriterion(self)
    
    def create_user_list_criterions(self,):
        return UserListCriterion(self)
    
    def create_bid_modifier(self,):
        return BidModifier(self)
    
    def get_keywords(self,):
        self.operator_container.selector[0]['fields'] = Keyword.fields
        self.operator_container.selector[0]['predicates'][0]['values'][0] = self.ad_group_id
        results = self.service_container.service_criterion.get(self.operator_container.selector)['entries']
        keywords = [ Keyword(self, result['criterion']['id']) for result in results if result['criterionUse'] == 'BIDDABLE' and result['criterion']['type']=='KEYWORD' ]
#         for result in results:
#             keyword_id = result['criterion']['id']
#             keyword = Keyword(self, keyword_id)
        return keywords
    
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


# In[45]:


class Keyword(object):
    fields = [
        'Id', 'KeywordText', 'Status', 'KeywordMatchType', 'SystemServingStatus', 'FirstPageCpc', 'FirstPositionCpc',
        'BidModifier', 'QualityScore'
    ]
    def __init__(self, AdGroup, keyword_id):
        self.ad_group = AdGroup
        self.keyword_id = keyword_id
        self.operator_container = OperatorContainer()
        
    def retrieve(self,):
        self.operator_container.selector[0]['fields'] = Keyword.fields
        self.operator_container.selector[0]['predicates'][0]['values'][0] = self.ad_group.ad_group_id
        predicate_keyword = {'field': 'Id', 'operator': 'EQUALS', 'values':[self.keyword_id]}
        self.operator_container.selector[0]['predicates'].append(predicate_keyword)
        result = self.ad_group.service_container.service_criterion.get(self.operator_container.selector)
        if 'entries' in result and len(result['entries']) != 0:
            self.text = result['entries'][0]['criterion']['text']
            self.match_type = result['entries'][0]['criterion']['matchType']
            self.status = result['entries'][0]['userStatus']
            self.first_page_cpc = result['entries'][0]['firstPageCpc']['amount']['microAmount']/1000000 if result['entries'][0]['firstPageCpc'] else 0
            self.first_position_cpc = result['entries'][0]['firstPositionCpc']['amount']['microAmount']/1000000 if result['entries'][0]['firstPositionCpc'] else 0
            self.keyword_dict = { 
                'ad_group_id': self.ad_group.ad_group_id, 'keyword_id': self.keyword_id, 'text': self.text, 'match_type': self.match_type,
                'status': self.status, 'first_page_cpc': self.first_page_cpc, 'first_position_cpc': self.first_position_cpc
            }
            return self
        
    def update_status(self, status):
        self.operator_container.criterion['id'] = self.keyword_id
        self.operator_container.criterion['xsi_type'] = 'Keyword'
        self.operator_container.criterion_operand['criterion'] = self.operator_container.criterion
        self.operator_container.criterion_operand['adGroupId'] = self.ad_group.ad_group_id
        self.operator_container.criterion_operand['xsi_type'] = 'BiddableAdGroupCriterion'
        self.operator_container.criterion_operand['userStatus'] = status
        self.operator_container.operation['operand'] = self.operator_container.criterion_operand
        self.operator_container.operation['operator'] = 'SET'
        self.operator_container.operations.append(self.operator_container.operation)
        result = self.ad_group.service_container.service_criterion.mutate(self.operator_container.operations)
        return result

    def update_bid(self, bid_micro_amount):
        self.operator_container.criterion['id'] = self.keyword_id
        self.operator_container.criterion.pop('xsi_type')
        self.operator_container.criterion_operand['criterion'] = self.operator_container.criterion
        self.operator_container.criterion_operand['xsi_type'] = 'BiddableAdGroupCriterion'
        self.operator_container.criterion_operand['adGroupId'] = self.ad_group.ad_group_id
        self.operator_container.criterion_operand['biddingStrategyConfiguration'] = {
            'bids': [{
                'xsi_type': 'CpcBid',
                'bid': {
                    'microAmount': int(bid_micro_amount * 1000000),
                }
            }]
        }
        self.operator_container.operation['operand'] = self.operator_container.criterion_operand
        self.operator_container.operation['operator'] = 'SET'
        self.operator_container.operations.append(self.operator_container.operation)
        result = self.ad_group.service_container.service_criterion.mutate(self.operator_container.operations)
        return result


# In[39]:


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
        self.campaign_id = result['entries'][0]['campaignId']
        return result['entries']
    
    def update_bid(self, bid_micro_amount):
        self.operation_container.operand['id'] = self.ad_group.ad_group_id
        self.operation_container.operand['biddingStrategyConfiguration'] = {
            'bids': [{
                'xsi_type': 'CpcBid',
                'bid': {
                    'microAmount': int(bid_micro_amount * 1000000),
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


# In[40]:


class Criterion(object):
    fields = [
        'AdGroupId', 'CriteriaType', 'UserInterestId', 'UserInterestName', 'UserListId', 'VerticalId', 'LabelIds',
        'BiddingStrategyType', 'BiddingStrategySource', 'BiddingStrategyId', 'Status',
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
    
    def retrieve_status(self,):
        entries = self.ad_group.service_container.service_criterion.get(self.operation_container.selector)['entries']
        biddable_criterions = [ entry for i, entry in enumerate(entries) if entry["AdGroupCriterion.Type"] == 'BiddableAdGroupCriterion']
        biddable_criterions = [ entry for i, entry in enumerate(entries) if entry['criterion']['type']==self.criterion_type or self.criterion_type is None]
        biddable_status = [ dict(zip([entry['criterion']['userListId']],[entry['userStatus']])) for i, entry in enumerate(entries) if 'userStatus' in entry]
        return biddable_status

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
            self.operation_container.criterion['id'] = criterion['id'] if criterion['xsi_type'] != 'Keyword' else None
            self.operation_container.criterion_operand['adGroupId'] = self.ad_group.ad_group_id
            self.operation_container.criterion_operand['criterion'] = criterion
            self.operation_container.operation['operand'] = self.operation_container.criterion_operand
            self.operation_container.operation['operator'] = 'SET' if  criterion.get('id') in criterion_id_list else 'ADD'
            self.operation_container.operations.append(copy.deepcopy(self.operation_container.operation))
        if len(self.operation_container.operations)!=0:
            result = self.ad_group.service_container.service_criterion.mutate(self.operation_container.operations)
            self.operation_container.operations=[]
            return result


# In[21]:


class BasicCriterion(Criterion):
    def __init__(self, AdGroup):
        super().__init__(AdGroup)
        self.operation_container.criterion['xsi_type'] = 'AgeRange'
        self.criterion_type = 'AGE_RANGE'

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
            self.operation_container.criterion['id'] = criterion['id'] if criterion['Criterion.Type'] != 'Keyword' else None
            self.operation_container.criterion_operand['adGroupId'] = self.ad_group.ad_group_id
            self.operation_container.criterion_operand['criterion'] = criterion
            self.operation_container.operation['operand'] = self.operation_container.criterion_operand
            self.operation_container.operation['operator'] = 'SET' if  criterion['id'] in criterion_id_list else 'ADD'
            self.operation_container.operations.append(copy.deepcopy(self.operation_container.operation))
        if len(self.operation_container.operations)!=0:
            result = self.ad_group.service_container.service_criterion.mutate(self.operation_container.operations)
            self.operation_container.operations=[]
            return result


# In[32]:


class UserInterestCriterion(Criterion):
    def __init__(self, AdGroup):
        super().__init__(AdGroup)
        self.operation_container.criterion['xsi_type'] = 'CriterionUserInterest'
        self.criterion_type = 'USER_INTEREST'


# In[16]:


class UserVerticalCriterion(Criterion):
    def __init__(self, AdGroup):
        super().__init__(AdGroup)
        self.operation_container.criterion['xsi_type'] = 'Vertical'
        self.criterion_type = 'VERTICAL'


# In[23]:


class UserListCriterion(Criterion):
    def __init__(self, AdGroup):
        super().__init__(AdGroup)
        self.operation_container.criterion['xsi_type'] = 'CriterionUserList'
        self.criterion_type = 'USER_LIST'
    
    def make(self, user_list_id):
        biddable_criterions, negative_criterions = self.retrieve()
        all_criterions = biddable_criterions + negative_criterions
        user_list_id_list = [ all_criterion['userListId'] for all_criterion in all_criterions if all_criterion['userListId'] ]
        criterion_id_list = [ all_criterion['id'] for all_criterion in all_criterions if all_criterion['id'] ]
        biddable_status = self.retrieve_status()
        status = [ status[userlist_id] for status in biddable_status if userlist_id in status ]
        status = status[0] if any(status) else 'PAUSED'
        # Make criterion
#         self.operation_container.criterion.update({'userListId':None})
        self.operation_container.criterion['userListId'] = user_list_id
        # Put operations > operand > criterion together
        self.operation_container.criterion_operand['adGroupId'] = self.ad_group.ad_group_id
        self.operation_container.criterion_operand['criterion'] = self.operation_container.criterion
        self.operation_container.criterion_operand['xsi_type'] = 'BiddableAdGroupCriterion'
        self.operation_container.operation['operand'] = self.operation_container.criterion_operand
        if user_list_id in user_list_id_list:
            self.operation_container.operation['operator'] = 'SET'
            self.operation_container.criterion_operand['userStatus'] = status
            self.operation_container.criterion_operand['criterion']['id'] = criterion_id_list[user_list_id_list.index(user_list_id)]
        else:
            self.operation_container.operation['operator'] = 'ADD'
            self.operation_container.criterion_operand['criterion'].pop('id', None)
            self.operation_container.criterion_operand.pop('id', None)
            
        self.operation_container.operations.append( copy.deepcopy( self.operation_container.operation ) )
        print(self.operation_container.operations)
        result = self.ad_group.service_container.service_criterion.mutate(self.operation_container.operations)
        self.operation_container.operations=[]
        return result


# In[24]:


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
        result = self.retrieve()
        if len(result) != 0:
            self.ad_group_id = ad_group_id
            self.operator_container.operations = []
            self.operator_container.operation['operator'] = 'SET' if result else 'ADD'
            self.operator_container.operand.pop('id')
            self.operator_container.operand['adGroupId'] = self.ad_group_id
#             print(result[0]['status'])
            self.operator_container.operand['status'] = result[0]['status']
            self.operator_container.operand['ad'] = {}
            self.operator_container.operand['ad']['id'] = self.ad_id
            self.operator_container.operation['operand'] = self.operator_container.operand
            self.operator_container.operations.append(self.operator_container.operation)
            result = self.service_ad.mutate(self.operator_container.operations)
            return result


# In[25]:


class BidModifier(object):
    fields = ['CampaignId', 'AdGroupId', 'BidModifier', 'Id', 'PlatformName']
    def __init__(self, AdGroup):
        self.ad_group = AdGroup
        self.operation_container = OperatorContainer()
        
    def retrieve(self,):
        self.operation_container.selector_bid_modifier['fields'] = self.fields
        self.operation_container.selector_bid_modifier['predicates'][0]['values'][0] = self.ad_group.ad_group_id
        resp = self.ad_group.service_container.service_bid_modifier.get(self.operation_container.selector_bid_modifier)
        return resp['entries']
    
    def update(self, bid_modifier_dict):
        self.operation_container.operand.pop('id', None)
        for device in bid_modifier_dict:
            device_id = Device.__dict__.get(device)
            bid_modifier_ratio = bid_modifier_dict[device]
            self.operation_container.criterion['xsi_type'] = 'Platform'
            self.operation_container.criterion['id'] = device_id
            
            self.operation_container.operand['adGroupId'] = self.ad_group.ad_group_id
            self.operation_container.operand['criterion'] = self.operation_container.criterion
            self.operation_container.operand['bidModifier'] = bid_modifier_ratio
            
            self.operation_container.operation['operator'] = 'ADD'
            self.operation_container.operation['operand'] = self.operation_container.operand
            
            self.operation_container.operations.append(copy.deepcopy(self.operation_container.operation))
        resp = self.ad_group.service_container.service_bid_modifier.mutate(self.operation_container.operations)
        self.operation_container.operations = []
        return resp


# In[14]:


# !jupyter nbconvert --to script google_adwords_controller.ipynb


# In[35]:


# service_container = AdGroupServiceContainer(customer_id=8845038097)

# ad_group = AdGroup(service_container, 77398403505)

# keywords = ad_group.get_keywords()

# import gdn_custom_audience as custom_audience
# optimized_list_dict_list, all_converters_dict_list = custom_audience.get_campaign_custom_audience(6451753179)

# optimized_list_dict_list

# all_converters_dict_list

