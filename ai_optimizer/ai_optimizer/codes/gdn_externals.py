#!/usr/bin/env python
# coding: utf-8

# In[96]:


import gdn_datacollector
import datetime
import gdn_db
from googleads import adwords
import pandas as pd
import copy
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
DATABASE = "dev_gdn"
DATE = datetime.datetime.now().date()
AGE_RANGE_LIST = [503001,503002,503003,503004,503005,503006,503999,]


# In[90]:


class Index:
    criteria_column = {
        'URL': 'url_display_name',
        'AUDIENCE': 'criterion_id',
        'AGE_RANGE': 'criterion_id',
        'DISPLAY_KEYWORD': 'Keyword_id',}
    criteria_type = {   
        'URL': 'Placement',
        'AUDIENCE': 'CriterionUserInterest',
        'AGE_RANGE': 'AgeRange',
        'DISPLAY_KEYWORD': 'Keyword',}
    score = {  
        'URL': 'url',
        'CRITERIA': 'criteria',
        'AUDIENCE': 'audience',
        'AGE_RANGE': 'age_range',
        'DISPLAY_KEYWORD': 'display_keyword',}


# In[79]:


class Retrive(object):
    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
        self.adwords_client.SetClientCustomerId(self.customer_id)
        
    def params(self, ad_group_id=None, param_type=None):
        selector= [{
            'fields': None,
            'predicates': [{
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values':[None]
            }]
        }]
        if ad_group_id:
            selector[0]['predicates'][0]['values'] = [ad_group_id]
            if param_type == 'ad':
                ad_params_list = []
                ad_criteria = dict()
                _FIELDS = ['AdGroupId', 'HeadlinePart1', 'HeadlinePart2', 'DisplayUrl', 'CreativeFinalUrls', 'Description', 'Url']
                selector[0]['fields'] = _FIELDS
                ad_group_service = self.adwords_client.GetService('AdGroupAdService', version='v201809')
                ad_params = ad_group_service.get(selector)
                for entry in ad_params['entries']:
                    ad_criteria['id'] = entry['ad']['id']
                    ad_params_list.append(copy.deepcopy(ad_criteria))
                return ad_params_list
            
            elif param_type == 'ad_group':
                _FIELDS = ['CampaignId', 'AdGroupId', 'Name', 'CpcBid', 'BiddingStrategyId','BiddingStrategyName','BiddingStrategySource', 'BiddingStrategyType']
                ad_group_service = self.adwords_client.GetService('AdGroupService', version='v201809')
                selector[0]['fields'] = _FIELDS
                ad_group_params = ad_group_service.get(selector)
                return ad_group_params    
    
    def criterion(self, ad_group_id=None):
        _FIELDS = ['AdGroupId', 'CriteriaType', 'UserInterestId', 'UserInterestName', 'UserListId', 'LabelIds']
        selector= [{
            'fields': _FIELDS,
            'predicates': [{
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values':[ad_group_id]
            }]
        }]
        ad_group_service = self.adwords_client.GetService('AdGroupCriterionService', version='v201809')
        ad_group_criterion = ad_group_service.get(selector)
        return ad_group_criterion
        
        


# In[ ]:


class Make(object):
    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
        self.adwords_client.SetClientCustomerId(self.customer_id)
    
#     class Criterion(object):
        
    def ad_group(self, ad_group_id):
        
    def criterion(self, ad_group_id=None, ad_group_criterion=None, by=None):
        if not ad_group_id or not ad_group_criterion:
            raise ValueError('new_adgroup_id and old_ad_group_criterion is Required.')
        biddable_criterion = dict()
        negative_criterion = dict()
        biddable_criterion['adGroupId'] = adgroup_id
        negative_criterion['adGroupId'] = adgroup_id

        biddable_ad_group_criteria_list = list()
        negative_ad_group_criteria_list = list()
        for entry in ad_group_criterion['entries']:
            criterion = dict()
            if 'AgeRange' in entry['criterion']['Criterion.Type']:
                criterion['id'] = entry['criterion']['id']
                criterion['xsi_type'] = entry['criterion']['Criterion.Type']

            elif 'Placement'  in entry['criterion']['Criterion.Type']:
                criterion['id'] = entry['criterion']['id']
                criterion['url'] = entry['criterion']['url']
                criterion['xsi_type'] = entry['criterion']['Criterion.Type']

            elif 'Keyword'  in entry['criterion']['Criterion.Type']:
                criterion['id'] = entry['criterion']['id']
                criterion['matchType'] = entry['criterion']['matchType']
                criterion['text'] = entry['criterion']['text']
                criterion['xsi_type'] = entry['criterion']['Criterion.Type']

            elif 'CriterionUserInterest'  in entry['criterion']['Criterion.Type']:
                criterion['id'] = entry['criterion']['id']
                criterion['userInterestId'] = entry['criterion']['userInterestId']
                criterion['xsi_type'] = entry['criterion']['Criterion.Type']
            else:
                pass
            if criterion:
                if 'BiddableAdGroupCriterion' in entry['AdGroupCriterion.Type']:
                    biddable_criterion['xsi_type'] = entry['AdGroupCriterion.Type']
                    biddable_criterion['criterion'] = criterion
                    biddable_ad_group_criteria_list.append(copy.deepcopy(biddable_criterion))

                elif 'NegativeAdGroupCriterion' in entry['AdGroupCriterion.Type']:
                    negative_criterion['xsi_type'] = entry['AdGroupCriterion.Type']
                    negative_criterion['criterion'] = criterion
                    negative_ad_group_criteria_list.append(copy.deepcopy(negative_criterion))
        return biddable_ad_group_criteria_list, negative_ad_group_criteria_list
    
    


# In[104]:


def get_sorted_adgroup(campaign):
    mydb = gdn_db.connectDB(DATABASE)
    try:
        df = pd.read_sql(
            "select * from adgroup_score where campaign_id=%s" % (campaign), con=mydb)
        df = df[df.request_time.dt.date == DATE].sort_values(
            by=['score'], ascending=False)
        adgroup_list = df['adgroup_id'].unique()
    except:
        df_camp = gdn_db.get_campaign(campaign)
        destination_type = df_camp['destination_type'].iloc[0]
        adgroup_list = gdn_datacollector.Campaign(
            df_camp['customer_id'][df_camp.campaign_id==campaign].iloc[0],
            campaign,
            destination_type).get_adgroup_id_list()
    return adgroup_list.tolist()

def split_adgroup_list(adgroup_list):
    import math
    adgroup_list.sort(reverse=True)
    half = math.ceil(len(adgroup_list) / 2)
    return adgroup_list[:half], adgroup_list[half:]

def retrieve_ad_group_params(client, ad_group_id):
    _FIELDS = ['CampaignId', 'AdGroupId', 'Name', 'CpcBid', 'BiddingStrategyId','BiddingStrategyName','BiddingStrategySource', 'BiddingStrategyType']
    selector= [{
        'fields': _FIELDS,
    #             'dateRange': {'min': '20190301','max': '20190401'},
        'predicates': [
            {
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values':[ad_group_id]
            }
        ]
    }]
    ad_group_service = client.GetService('AdGroupService', version='v201809')
    ad_group_params = ad_group_service.get(selector)
    return ad_group_params

def make_ad_group(client, ad_group_id):
    ad_group_params = retrieve_ad_group_params(client, ad_group_id)
    campaign_id = ad_group_params['entries'][0]['campaignId']
    name = ad_group_params['entries'][0]['name']
    xsi_type = ad_group_params['entries'][0]['biddingStrategyConfiguration']['bids'][0]['Bids.Type']
    micro_amount = ad_group_params['entries'][0]['biddingStrategyConfiguration']['bids'][0]['bid']['microAmount']
    operations = [{
        'operator': 'ADD',
        'operand': {
            'campaignId': campaign_id,
            'name':  'Mutant {}'.format(name),
            'status': 'ENABLED',
            'biddingStrategyConfiguration': {
                'bids': [
                    {
                        'xsi_type': xsi_type,
                        'bid': {
                            'microAmount': micro_amount
                        }
                    }
                ]
            }
        }
    }]
    ad_group_service = client.GetService('AdGroupService', version='v201809')
    ad_groups = ad_group_service.mutate(operations)
    return ad_groups

def retrive_ad_params(client, ad_group_id):
    ad_params_list = []
    ad_criteria = dict()
    _FIELDS = ['AdGroupId', 'HeadlinePart1', 'HeadlinePart2', 'DisplayUrl', 'CreativeFinalUrls', 'Description', 'Url']
    selector= [{
        'fields': _FIELDS,
        'predicates': [
            {
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values':[ad_group_id]
            }
        ]
    }]
    ad_service = client.GetService('AdGroupAdService', version='v201809')
    ad_params = ad_service.get(selector)
    for entry in ad_params['entries']:
        ad_criteria['id'] = entry['ad']['id']
        ad_params_list.append(copy.deepcopy(ad_criteria))
    return ad_params_list

def assign_ad_to_ad_group(client, ad_group_id, ad_params_list):
    operations = []
    for ad_params in ad_params_list:
        operations.append({
            'operator': 'ADD',
            'operand': {
                'adGroupId': ad_group_id,
                'status': 'ENABLED',
                'ad': ad_params
            }
        })
    ad_group_service = client.GetService('AdGroupAdService', version='v201809')
    ad_groups = ad_group_service.mutate(operations)
    return ad_groups

def retrieve_ad_group_criterion(client, ad_group_id):
    _FIELDS = ['AdGroupId', 'CriteriaType', 'UserInterestId', 'UserInterestName', 'UserListId', 'LabelIds']
    selector= [{
        'fields': _FIELDS,
    #             'dateRange': {'min': '20190301','max': '20190401'},
        'predicates': [
            {
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values':[ad_group_id]
            }
        ]
    }]
    ad_group_service = client.GetService('AdGroupCriterionService', version='v201809')
    ad_group_criterion = ad_group_service.get(selector)
    return ad_group_criterion

def make_adgroup_criterion(new_adgroup_id = None, old_ad_group_criterion=None):
    if new_adgroup_id is None and old_ad_group_criterion is None:
        raise ValueError('new_adgroup_id and old_ad_group_criterion is Required.')
    biddable_criterion = dict()
    negative_criterion = dict()
    biddable_criterion['adGroupId'] = new_adgroup_id
    negative_criterion['adGroupId'] = new_adgroup_id

    biddable_ad_group_criteria_list = list()
    negative_ad_group_criteria_list = list()
    for entry in old_ad_group_criterion['entries']:
        criterion = dict()
        if 'AgeRange' in entry['criterion']['Criterion.Type']:
            criterion['id'] = entry['criterion']['id']
            criterion['xsi_type'] = entry['criterion']['Criterion.Type']
            
        elif 'Placement'  in entry['criterion']['Criterion.Type']:
            criterion['id'] = entry['criterion']['id']
            criterion['url'] = entry['criterion']['url']
            criterion['xsi_type'] = entry['criterion']['Criterion.Type']

        elif 'Keyword'  in entry['criterion']['Criterion.Type']:
            criterion['id'] = entry['criterion']['id']
            criterion['matchType'] = entry['criterion']['matchType']
            criterion['text'] = entry['criterion']['text']
            criterion['xsi_type'] = entry['criterion']['Criterion.Type']

        elif 'CriterionUserInterest'  in entry['criterion']['Criterion.Type']:
            criterion['id'] = entry['criterion']['id']
            criterion['userInterestId'] = entry['criterion']['userInterestId']
            criterion['xsi_type'] = entry['criterion']['Criterion.Type']
        else:
            pass
        if criterion:
            if 'BiddableAdGroupCriterion' in entry['AdGroupCriterion.Type']:
                biddable_criterion['xsi_type'] = entry['AdGroupCriterion.Type']
                biddable_criterion['criterion'] = criterion
                biddable_ad_group_criteria_list.append(copy.deepcopy(biddable_criterion))

            elif 'NegativeAdGroupCriterion' in entry['AdGroupCriterion.Type']:
                negative_criterion['xsi_type'] = entry['AdGroupCriterion.Type']
                negative_criterion['criterion'] = criterion
                negative_ad_group_criteria_list.append(copy.deepcopy(negative_criterion))
    return biddable_ad_group_criteria_list, negative_ad_group_criteria_list
 
def assign_criterion_to_ad_group(client, ad_group_id, ad_group_criterion):
    '''
    an example of addong 4 type of criterion to new adgroup,
    if trying to exclude certain criterions, 
    change 'xsi_type' from 'BiddableAdGroupCriterion' to 'NegativeAdGroupCriterion' in ad_group_criteria object
    '''
    # Initialize appropriate service.
    ad_group_criterion_service = client.GetService(
        'AdGroupCriterionService', version='v201809')
    # Create the ad group criteria.  
    # Create operations.
    operations = []
    for criterion in ad_group_criterion:
        operations.append({
            'operator': 'ADD',
            'operand': criterion
        })
    response = ad_group_criterion_service.mutate(operations)
    return response

def make_adgroup_criterion_by_score(campaign_id, new_ad_group_id):
    criterions=[]
    for criteria in Index.score.keys():
        df = gdn_db.get_table(campaign_id=campaign_id, table=Index.score[criteria]+"_score").head()
        if criteria == 'AUDIENCE' and not df.empty:
            df['audience'] = df.audience.str.split('::', expand = True)[1]
        if not df.empty:
#             print(criteria)
            sub_criterions = []
            df = df.sort_values(by=['score'], ascending=False).reset_index(drop=True).drop_duplicates([Index.criteria_column[criteria]])
            criterion = {
                'adGroupId': new_ad_group_id,
                'xsi_type': None,
                'criterion': {
                    'xsi_type': None
                }
            }
            if criteria == 'AGE_RANGE':
                criterion['criterion']['xsi_type'] = Index.criteria_type[criteria]
                for i, criterion_id in enumerate( AGE_RANGE_LIST ):
                    criterion['criterion']['id'] = criterion_id
                    if criterion_id not in df[Index.criteria_column[criteria]].unique() and criterion_id != 503999:
                        criterion['xsi_type'] = 'NegativeAdGroupCriterion'
                    else:
                        criterion['xsi_type'] = 'BiddableAdGroupCriterion'
                    sub_criterions.append(copy.deepcopy(criterion))                    
            else:
                criterion['criterion']['xsi_type'] = Index.criteria_type[criteria]
                for i, criterion_id in enumerate( df[Index.criteria_column[criteria]] ):
                    if Index.criteria_type[criteria] == 'Placement':
                        criterion['criterion']['id'] = criterion_id
                        criterion['criterion']['url'] = df['url_display_name'].iloc[i]

                    elif Index.criteria_type[criteria] == 'Keyword':
#                         criterion['criterion']['id'] = criterion_id
                        criterion['criterion']['matchType'] = 'BROAD'
                        criterion['criterion']['text'] = df['keyword'].iloc[i]

                    elif Index.criteria_type[criteria] == 'CriterionUserInterest':
                        criterion['criterion']['userInterestId'] = df['audience'].iloc[i]
                        criterion['criterion']['id'] = criterion_id
                    sub_criterions.append(copy.deepcopy(criterion))
            criterions = criterions+sub_criterions
    return criterions

def make_adgroup_with_criterion(client, old_ad_group_id):
    # Params
    ad_group_params = make_ad_group(client, old_ad_group_id)
    new_ad_group_id = ad_group_params['value'][0]['id']

    # Criterion by Score
    retrieve_ad_group_criterion(client, old_ad_group_id)
    new_ad_group_criterion = make_adgroup_criterion_by_score(campaign_id, new_ad_group_id)
    result = assign_criterion_to_ad_group(client, new_ad_group_id, new_ad_group_criterion)
    
    # Criterion by former adgroup
#     old_ad_group_criterion = retrieve_ad_group_criterion(client, old_ad_group_id)
#     biddable_criterion, negative_criterion = make_adgroup_criterion(new_ad_group_id, old_ad_group_criterion)
#     new_ad_group_criterion = biddable_criterion + negative_criterion
    # Assign Ad Params
    ad_params_list = retrive_ad_params(client, old_ad_group_id)
    assign_ad_to_ad_group(client, new_ad_group_id, ad_params_list)
    return new_ad_group_criterion
#     return result


# In[98]:



def main():
    starttime = datetime.datetime.now()
    df_camp = gdn_db.get_campaign()
    campaign_id_list = df_camp['campaign_id'].unique()
    for campaign_id in campaign_id_list:
        client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
        client.SetClientCustomerId(df_camp['customer_id'][df_camp.campaign_id==campaign_id].iloc[0])
        adgroup_list = get_sorted_adgroup(campaign_id)
        adgroup_for_copy, adgroup_for_off = split_adgroup_list(adgroup_list)
        for adgroup_id in adgroup_for_copy:
            make_adgroup_with_criterion(adgroup_id)

        for adgroup_id in adgroup_for_off:
            adgroup = gdn_datacollector.AdGroup( 
                df_camp['customer_id'][df_camp.campaign_id==campaign_id].iloc[0],
                df_camp['campaign_id'][df_camp.campaign_id==campaign_id].iloc[0],
                df_camp['destination_type'][df_camp.campaign_id==campaign_id].iloc[0],
                adgroup_id )
            adgroup.update_status(client=client)
    return
# def main(campaign_id):
#     df_camp = gdn_db.get_campaign(campaign_id)
#     client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
    
#     client.SetClientCustomerId(df_camp['customer_id'].iloc[0])
#     adgroup_list = get_sorted_adgroup(campaign_id)
#     adgroup_for_copy, adgroup_for_off = split_adgroup_list(adgroup_list)
#     for adgroup_id in adgroup_for_copy:
# #         make_adgroup_with_criterion(client, adgroup_id)
#         new_ad_group_criterion = make_adgroup_with_criterion_2(client, adgroup_id)
#     return


# In[50]:


if __name__=="__main__":
    main()


# In[63]:




