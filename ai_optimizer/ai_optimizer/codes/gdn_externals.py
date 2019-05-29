#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gdn_datacollector
from gdn_datacollector import Campaign
from gdn_datacollector import AdGroup
from gdn_datacollector import DatePreset
import datetime
import gdn_db
from googleads import adwords
import pandas as pd
import copy
import math
IS_DEBUG = True
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
DATABASE = "dev_gdn"
DATE = datetime.datetime.now().date()
DATETIME = datetime.datetime.now()
AGE_RANGE_LIST = [503001,503002,503003,503004,503005,503006,503999,]


# In[2]:


class Index:
    criteria_column = {
        'URL': 'url_display_name',
        'AUDIENCE': 'criterion_id',
#         'AGE_RANGE': 'criterion_id',
        'DISPLAY_KEYWORD': 'keyword_id',}
    criteria_type = {   
        'URL': 'Placement',
        'AUDIENCE': 'CriterionUserInterest',
#         'AGE_RANGE': 'AgeRange',
        'DISPLAY_KEYWORD': 'Keyword',}
    score = {  
        'URL': 'url',
        'CRITERIA': 'criteria',
        'AUDIENCE': 'audience',
#         'AGE_RANGE': 'age_range',
        'DISPLAY_KEYWORD': 'display_keyword',}


# In[3]:


class Retrieve(object):
    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
        self.adwords_client.SetClientCustomerId(self.customer_id)
        self.mutant_ad_group_id_list = []
        self.native_ad_group_id_list = []
        
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
    
    def criterion(self, ad_group_id=None, campaign_id=None):
        if ad_group_id:
            _FIELDS = ['AdGroupId', 'CriteriaType', 'UserInterestId', 'UserInterestName', 'UserListId', 'LabelIds']
            selector= [{
                'fields': _FIELDS,
                'predicates': [{
                    'field': 'AdGroupId',
                    'operator': 'EQUALS',
                    'values':[ad_group_id]
                }]
            }]
            service = self.adwords_client.GetService('AdGroupCriterionService', version='v201809')
        elif campaign_id:
            _FIELDS = ['CampaignId', 'CriteriaType', 'UserInterestId', 'UserInterestName', 'UserListId']
            selector= [{
                'fields': _FIELDS,
                'predicates': [{
                    'field': 'CampaignId',
                    'operator': 'EQUALS',
                    'values':[campaign_id]
                }]
            }]
            service = self.adwords_client.GetService('CampaignCriterionService', version='v201809')
        entries = service.get(selector)['entries']
        criterions = [ entries[i]['criterion'] for i, criterion in enumerate(entries) ]
        return criterions
    
    def generate_ad_group_id_list_type(self, campaign_id):
        _FIELDS = ['CampaignId', 'Name', 'AdGroupId']
        native_ad_group_id_list = []
        mutate_ad_group_id_list = []
        selector= [{
            'fields': _FIELDS,
            'predicates': [{
                'field': 'CampaignId',
                'operator': 'EQUALS',
                'values':[campaign_id]
            },
            {
                'field': 'Status',
                'operator': 'EQUALS',
                'values':'ENABLED'
            }]
        }]
        ad_group_service = self.adwords_client.GetService('AdGroupService', version='v201809')
        ad_group_criterion = ad_group_service.get(selector)
        for ad_group in ad_group_criterion['entries']:
            if 'Mutant' in ad_group['name'].split():
                mutate_ad_group_id_list.append( ad_group['id'] )
                
            else:
                native_ad_group_id_list.append( ad_group['id'] )
        self.mutant_ad_group_id_list = mutate_ad_group_id_list
        self.native_ad_group_id_list = native_ad_group_id_list
        return {
            'mutant': mutate_ad_group_id_list,
            'native': native_ad_group_id_list
        }


# In[4]:


class Make(object):
    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
        self.adwords_client.SetClientCustomerId(self.customer_id)
    
#     class Criterion(object):
        
    def ad_group(self, ad_group_id=None):
        ad_group_params = retrieve_ad_group_params(self.adwords_client, ad_group_id)
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
                    'bids': [{
                        'xsi_type': xsi_type,
                        'bid': {
                            'microAmount': micro_amount
                        }
                    }]
                }
            }
        }]
        ad_group_service = self.adwords_client.GetService('AdGroupService', version='v201809')
        ad_groups = ad_group_service.mutate(operations)
        return ad_groups
        
        
        
        
        
        
        
        
        
    def criterion(self, ad_group_id=None, ad_group_criterion=None, by_assessment=True):
        if not ad_group_id or not ad_group_criterion:
            raise ValueError('new_ad_group_id and old_criterion is Required.')
        if by_assessment:
            criterions=[]
            for criteria in Index.score.keys():
                df = gdn_db.get_table(campaign_id=campaign_id, table=Index.score[criteria]+"_score").head()
                if criteria == 'AUDIENCE' and not df.empty:
                    df['audience'] = df.audience.str.split('::', expand = True)[1]
                if not df.empty:
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
                                
                            elif Index.criteria_type[criteria] == 'CriterionUserList':
                                criterion['criterion']['id'] = criterion_id
                                criterion['criterion']['userListId'] = df['audience'].iloc[i]

                            elif Index.criteria_type[criteria] == 'CriterionCustomAffinity':
                                criterion['criterion']['id'] = criterion_id
                                criterion['criterion']['customAffinityId'] = df['audience'].iloc[i]
                            sub_criterions.append(copy.deepcopy(criterion))
                    criterions = criterions+sub_criterions
            return criterions
        else:
            biddable_criterion, negative_criterion = dict(), dict()
            biddable_criterion['adGroupId'], negative_criterion['adGroupId'] = ad_group_id, ad_group_id
            biddable_criteria_list, negative_criteria_list = list(), list()
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
                    
                elif 'CriterionUserList'  in entry['criterion']['Criterion.Type']:
                    criterion['id'] = entry['criterion']['id']
                    criterion['userListId'] = entry['criterion']['userListId']
                    criterion['xsi_type'] = entry['criterion']['Criterion.Type']
                    
                elif 'CriterionCustomAffinity'  in entry['criterion']['Criterion.Type']:
                    criterion['id'] = entry['criterion']['id']
                    criterion['customAffinityId'] = entry['criterion']['customAffinityId']
                    criterion['xsi_type'] = entry['criterion']['Criterion.Type']
                                        
                else:
                    pass
                if criterion:
                    if 'BiddableAdGroupCriterion' in entry['AdGroupCriterion.Type']:
                        biddable_criterion['xsi_type'] = entry['AdGroupCriterion.Type']
                        biddable_criterion['criterion'] = criterion
                        biddable_criteria_list.append(copy.deepcopy(biddable_criterion))

                    elif 'NegativeAdGroupCriterion' in entry['AdGroupCriterion.Type']:
                        negative_criterion['xsi_type'] = entry['AdGroupCriterion.Type']
                        negative_criterion['criterion'] = criterion
                        negative_criteria_list.append(copy.deepcopy(negative_criterion))
            return biddable_criteria_list, negative_criteria_list
    
    


# In[ ]:


# # init param retriever Retrieve
# retriever = Retrieve(customer_id)
# retriever.generate_ad_group_id_list_type(campaign_id)

# if achieving_rate < 1 and achieving_rate >= 0:
#     while len(retriever.mutant_ad_group_id_list) != 0:
#         for mutant_ad_group_id in retriever.mutant_ad_group_id_list:
#             Status(customer_id).update(ad_group_id=mutant_ad_group_id, status='PAUSED')
#         break
#     mutant_ad_group_id = make_empty_ad_group(adwords_client, campaign_id, retriever.native_ad_group_id_list)

#     # Assign criterion to mutant ad group
#     make_adgroup_with_criterion(adwords_client, campaign_id,
#                                 native_ad_group_id_list=retriever.native_ad_group_id_list,
#                                 mutant_ad_group_id_list=[mutant_ad_group_id])


# In[5]:


def get_sorted_adgroup(campaign_id):
    mydb = gdn_db.connectDB(DATABASE)
    try:
        df = pd.read_sql(
            "select * from adgroup_score where campaign_id=%s" % (campaign_id), con=mydb)
        df = df[df.request_time.dt.date == DATE].sort_values(
            by=['score'], ascending=False)
        adgroup_list = df['adgroup_id'].unique()
    except:
        df_camp = gdn_db.get_campaign(campaign_id)
        destination_type = df_camp['destination_type'][df_camp.campaign_id == campaign_id].iloc[0]
        customer_id = df_camp['customer_id'][df_camp.campaign_id == campaign_id].iloc[0]
        adgroup_list = Campaign( customer_id, campaign_id, destination_type).get_adgroup_id_list()
    mydb.close()
    return adgroup_list.tolist()


def split_adgroup_list(adgroup_list):
    import math
    adgroup_list.sort(reverse=True)
    half = math.ceil(len(adgroup_list) / 2)
    return adgroup_list[:half], adgroup_list[half:]

def modify_opt_result_db(campaign_id, is_optimized):
    #get date
    opt_date = datetime.datetime.now()
    #insert to table date and Ture for is_opt
    sql = "update campaign_target set is_optimized = '{}', optimized_date = '{}' where campaign_id = {}".format(is_optimized, opt_date, campaign_id)
    mydb = gdn_db.connectDB(DATABASE)
    mycursor = mydb.cursor()
    try:
        mycursor.execute(sql)
    except Exception as e:
        print('[gdn_externals] modify_opt_result_db: ', e)
    finally:
        mydb.commit()
        mydb.close()

def retrieve_ad_group_params(adwords_client, ad_group_id):
    _FIELDS = ['CampaignId', 'AdGroupId', 'Name', 'CpcBid', 'BiddingStrategyId',
               'BiddingStrategyName', 'BiddingStrategySource', 'BiddingStrategyType']
    selector = [{
        'fields': _FIELDS,
        #             'dateRange': {'min': '20190301','max': '20190401'},
        'predicates': [
            {
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values': [ad_group_id]
            }
        ]
    }]
    ad_group_service = adwords_client.GetService(
        'AdGroupService', version='v201809')
    ad_group_params = ad_group_service.get(selector)
    return ad_group_params


def make_ad_group(adwords_client, ad_group_id):
    ad_group_params = retrieve_ad_group_params(adwords_client, ad_group_id)
    campaign_id = ad_group_params['entries'][0]['campaignId']
    name = ad_group_params['entries'][0]['name']
    xsi_type = ad_group_params['entries'][0]['biddingStrategyConfiguration']['bids'][0]['Bids.Type']
    micro_amount = ad_group_params['entries'][0]['biddingStrategyConfiguration']['bids'][0]['bid']['microAmount']
    operations = [{
        'operator': 'ADD',
        'operand': {
            'campaignId': campaign_id,
            'name':  'Mutant {}-{}'.format(name, DATETIME),
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
    ad_group_service = adwords_client.GetService(
        'AdGroupService', version='v201809')
    new_ad_group_params = ad_group_service.mutate(operations)
#     new_ad_group_id = new_ad_group_params['value'][0]['id']
    return new_ad_group_params


def retrieve_ad_params(adwords_client, ad_group_id):
    ad_params_list = []
    ad_criteria = dict()
    _FIELDS = ['AdGroupId', 'HeadlinePart1', 'HeadlinePart2',
               'DisplayUrl', 'CreativeFinalUrls', 'Description', 'Url']
    selector = [{
        'fields': _FIELDS,
        'predicates': [
            {
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values': [ad_group_id]
            }
        ]
    }]
    ad_service = adwords_client.GetService(
        'AdGroupAdService', version='v201809')
    ad_params = ad_service.get(selector)
    for entry in ad_params['entries']:
        ad_criteria['id'] = entry['ad']['id']
        ad_params_list.append(copy.deepcopy(ad_criteria))
    return ad_params_list


def assign_ad_to_ad_group(adwords_client, ad_group_id, ad_params_list):
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
    ad_group_service = adwords_client.GetService(
        'AdGroupAdService', version='v201809')
    ad_groups = ad_group_service.mutate(operations)
    return ad_groups


def retrieve_ad_group_criterion(adwords_client, ad_group_id):
    _FIELDS = ['AdGroupId', 'CriteriaType', 'UserInterestId',
               'UserInterestName', 'UserListId', 'LabelIds', 'BiddingStrategyType', 'BiddingStrategySource', 'BiddingStrategyId',]
    selector = [{
        'fields': _FIELDS,
        #             'dateRange': {'min': '20190301','max': '20190401'},
        'predicates': [
            {
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values': [ad_group_id]
            }
        ]
    }]
    ad_group_service = adwords_client.GetService(
        'AdGroupCriterionService', version='v201809')
    ad_group_criterion = ad_group_service.get(selector)
    return ad_group_criterion

def make_criterion(new_ad_group_id, df,criteria):
    sub_criterions = []
    # make criterion stucture in order to upadate back to gdn platform
    criterion = {
        'adGroupId': new_ad_group_id,
        'xsi_type': None,
        'criterion': {
            'xsi_type': None
        }
    }
    if criteria == 'AGE_RANGE':
        criterion['criterion']['xsi_type'] = Index.criteria_type[criteria]
        for i, criterion_id in enumerate(AGE_RANGE_LIST):
            criterion['criterion']['id'] = criterion_id
            if criterion_id not in df[Index.criteria_column[criteria]].unique() and criterion_id != 503999:
                criterion['xsi_type'] = 'NegativeAdGroupCriterion'
            else:
                criterion['xsi_type'] = 'BiddableAdGroupCriterion'
            sub_criterions.append(copy.deepcopy(criterion))
    else:
        criterion['xsi_type'] = 'BiddableAdGroupCriterion'
        criterion['criterion']['xsi_type'] = Index.criteria_type[criteria]
        for i, criterion_id in enumerate(df[Index.criteria_column[criteria]]):
            if Index.criteria_type[criteria] == 'Placement':
                criterion['criterion']['id'] = criterion_id
                criterion['criterion']['url'] = df['url_display_name'].iloc[i]
            elif Index.criteria_type[criteria] == 'Keyword':
                criterion['criterion']['matchType'] = 'BROAD'
                criterion['criterion']['text'] = df['keyword'].iloc[i]
            elif Index.criteria_type[criteria] == 'CriterionUserInterest':
                criterion['criterion']['userInterestId'] = df['audience'].iloc[i]
                criterion['criterion']['id'] = criterion_id
            elif Index.criteria_type[criteria] == 'CriterionUserList':
                criterion['criterion']['id'] = criterion_id
                criterion['criterion']['userListId'] = df['audience'].iloc[i]
            elif Index.criteria_type[criteria] == 'CriterionCustomAffinity':
                criterion['criterion']['id'] = criterion_id
                criterion['criterion']['customAffinityId'] = df['audience'].iloc[i]
            sub_criterions.append(copy.deepcopy(criterion))
    return sub_criterions

def make_adgroup_criterion_by_score(campaign_id, new_ad_group_id,):
    biddable_criterions = []
    negative_criterions = []
    for criteria in Index.score.keys():
        # select score by campaign level
        df = gdn_db.get_table(campaign_id=campaign_id,
                              table=Index.score[criteria]+"_score")
        df['request_time'] = pd.to_datetime(df['request_time'])
        df = df[ df.request_time.dt.date == (datetime.datetime.now().date()-datetime.timedelta(1)) ]
        if criteria == 'AUDIENCE' and not df.empty:
            df['audience'] = df.audience.str.split('::', expand=True)[1]
        if not df.empty:
            # 
            sub_criterions = []
            df = df.sort_values(by=['score'], ascending=False).drop_duplicates(
                [ Index.criteria_column[criteria] ] ).reset_index(drop=True)
            half = math.ceil( len(df)/2 )
            biddable_df = df.iloc[:half]
            negative_df = df.iloc[half:]
            biddable_sub_criterion = make_criterion(new_ad_group_id, biddable_df,criteria)
            negative_sub_criterion = make_criterion(new_ad_group_id, negative_df,criteria)
    biddable_criterions += biddable_sub_criterion
    negative_criterions += negative_sub_criterion
    return biddable_criterions, negative_criterions

def make_empty_ad_group(adwords_client, campaign_id, ad_group_id_list):
    ad_group_params = make_ad_group(adwords_client, ad_group_id_list[0])
    new_ad_group_id = ad_group_params['value'][0]['id']
    return new_ad_group_id

def make_adgroup_with_criterion(adwords_client, update, campaign_id, native_ad_group_id_list=None):
    if native_ad_group_id_list:
    # Criterion by Score
        for native_id_idx, native_id in enumerate(native_ad_group_id_list):
            print('[native_id] ', native_id)
            biddable_criterions, negative_criterions = make_adgroup_criterion_by_score( campaign_id, native_id )
            
            for biddable in biddable_criterions:
                native_id = biddable['adGroupId']
                status = 'ENABLED'
                interest_id = biddable['criterion']['userInterestId']
                id = biddable['criterion']['id']
                update.criterion( ad_group_id=native_id, id=id, interest_id=interest_id, status=status, )
                
            for negative in negative_criterions:
                native_id = negative['adGroupId']
                status = 'PAUSED'
                interest_id = negative['criterion']['userInterestId']
                id = negative['criterion']['id']
                update.criterion( ad_group_id=native_id, id=id, interest_id=interest_id, status=status, )
            # Assign Ad Params
            try:
                ad_params_list = retrieve_ad_params(adwords_client, native_id)
                assign_ad_to_ad_group(adwords_client, native_id, ad_params_list)
            except Exception as e:
                print('[assign ad to ad_group]: ads already exist, ', e)
                pass
    return


# In[6]:


class Status(object):
    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
        self.adwords_client.SetClientCustomerId(self.customer_id)
    def update(self, campaign_id=None, ad_group_id=None, status='ENABLED'):
        if campaign_id and status:
            status_service = self.adwords_client.GetService('CampaignService', version='v201809')
            update_id = campaign_id
            
        elif ad_group_id and status:
            status_service = self.adwords_client.GetService('AdGroupService', version='v201809')
            update_id = ad_group_id
        operations = [{
            'operator': 'SET',
            'operand': {
                'id': update_id,
                'status': 'PAUSED'
            }
        }]
        result = status_service.mutate(operations)
        return result
    
class Update(object):
    operand = {
        'id': None,
        'status': 'PAUSED'
    }
    operations = [{
        'operator': 'SET',
        'operand': operand
    }]

    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
        self.adwords_client.SetClientCustomerId(self.customer_id)
    def campaign(self, id=None, status='ENABLED'):
        if id and status:
            status_service = self.adwords_client.GetService('CampaignService', version='v201809')
            self.operand['id'] = id
            self.operand['status'] = status
            self.operations[0]['operand'] = self.operand
        result = status_service.mutate(operations)
        return
    
    def ad_group(self, id=None, status='ENABLED'):
        if id and status:
            status_service = self.adwords_client.GetService('AdGroupService', version='v201809')
            self.operand['id'] = id
            self.operand['status'] = status
            self.operations[0]['operand'] = self.operand
        result = status_service.mutate(operations)
        return
    
    def criterion( self, ad_group_id=None, id=None, interest_id=None, status='ENABLED', ):
        ad_group_service = self.adwords_client.GetService(
            'AdGroupCriterionService', version='v201809')
        # Make operand
        self.operand = dict()
        self.operand['adGroupId'] = ad_group_id
        self.operand['xsi_type'] = 'BiddableAdGroupCriterion'
        self.operand['userStatus'] = status
        self.operand['criterion'] = list()
        # Make criterion
        criterion = {'id':None, 'xsi_type':None, 'userInterestId':None}
        criterion['id'] = id
        criterion['xsi_type'] = 'CriterionUserInterest'
        criterion['userInterestId'] = interest_id
        # Put operations > operand > criterion together
        self.operand['criterion'].append(criterion)
        self.operations[0]['operand'] = self.operand
        ad_group_service.mutate(self.operations)
        return


# In[7]:



def main(campaign_id=None):
    starttime = datetime.datetime.now()
    adwords_client= adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
    if not campaign_id:
        df_camp = gdn_db.get_campaign_is_running()
        campaign_id_list = df_camp['campaign_id'].unique()
        for campaign_id in campaign_id_list:
            customer_id = df_camp['customer_id'][df_camp.campaign_id==campaign_id].iloc[0]
            destination_type = df_camp['destination_type'][df_camp.campaign_id==campaign_id].iloc[0]
            daily_target = df_camp['daily_target'][df_camp.campaign_id==campaign_id].iloc[0]
            adwords_client.SetClientCustomerId( customer_id )
            # Decide optimization target by destination type
            if destination_type == 'CONVERSIONS':
                target = 'conversions'
            elif destination_type == 'LINK_CLICKS':
                target = 'clicks'
            # Init datacollector Campaign
            camp = Campaign(customer_id, campaign_id, destination_type)
            day_dict = camp.get_campaign_insights(adwords_client,
                date_preset=DatePreset.yesterday)
            lifetime_dict = camp.get_campaign_insights(adwords_client,
                date_preset=DatePreset.lifetime)
            # Adjust initial bids
            handle_initial_bids(campaign_id, day_dict['spend'], day_dict['budget'])
            target = int( day_dict[target] )
            lifetime_target = int( lifetime_dict[target] )
            achieving_rate = target / daily_target
            print('[campaign_id]', campaign_id, )
            print('[achieving rate]', achieving_rate, '[target]', target, '[daily_target]', daily_target)
            
            # Init param retriever Retrieve
            retriever = Retrieve(customer_id)
            retriever.generate_ad_group_id_list_type(campaign_id)
            
            if achieving_rate < 1 and achieving_rate >= 0:
                if destination_type == 'CONVERSIONS':
                    update = Update(customer_id)
                    # Assign criterion to mutant ad group
                    make_adgroup_with_criterion(adwords_client, update, campaign_id,
                                                native_ad_group_id_list=retriever.native_ad_group_id_list,)
                elif destination_type == 'LINK_CLICKS':
                    mutant_ad_group_id = make_empty_ad_group(adwords_client, campaign_id, retriever.native_ad_group_id_list)
                    make_adgroup_with_criterion(adwords_client, update, campaign_id,
                                                native_ad_group_id_list=[mutant_ad_group_id],)
                    
                modify_opt_result_db(campaign_id , True)
            else:
                modify_opt_result_db(campaign_id , False)
    else:
        df_camp = gdn_db.get_campaign_is_running(campaign_id)
        if not df_camp.empty:
            customer_id = df_camp['customer_id'][df_camp.campaign_id==campaign_id].iloc[0]
            destination_type = df_camp['destination_type'][df_camp.campaign_id==campaign_id].iloc[0]
            daily_target = df_camp['daily_target'][df_camp.campaign_id==campaign_id].iloc[0]
            adwords_client.SetClientCustomerId( customer_id )
            # Decide optimization target by destination type
            if destination_type == 'CONVERSIONS':
                target = 'conversions'
            elif destination_type == 'LINK_CLICKS':
                target = 'clicks'
            # Init datacollector Campaign
            camp = Campaign(customer_id, campaign_id, destination_type)
            day_dict = camp.get_campaign_insights(adwords_client,
                date_preset=DatePreset.yesterday)
            lifetime_dict = camp.get_campaign_insights(adwords_client,
                date_preset=DatePreset.lifetime)
            target = int( day_dict[target] )
            print(day_dict)
            return
            achieving_rate = target / daily_target
            print('[campaign_id]', campaign_id, )
            print('[achieving rate]', achieving_rate, '[target]', target, '[daily_target]', daily_target)
            
            # Init param retriever Retrieve
            retriever = Retrieve(customer_id)
            retriever.generate_ad_group_id_list_type(campaign_id)
            
            if achieving_rate < 1 and achieving_rate >= 0:
                update = Update(customer_id)
                if destination_type == 'CONVERSIONS':
                    # Assign criterion to native ad group
                    make_adgroup_with_criterion(adwords_client, update, campaign_id,
                                                native_ad_group_id_list=retriever.native_ad_group_id_list,)
                elif destination_type == 'LINK_CLICKS':
                    # Make empty mutant ad group
                    mutant_ad_group_id = make_empty_ad_group(adwords_client, campaign_id, retriever.native_ad_group_id_list)
                    # Assign criterion to native ad group
                    make_adgroup_with_criterion(adwords_client, update, campaign_id,
                                                native_ad_group_id_list=[mutant_ad_group_id],)
                    
                modify_opt_result_db(campaign_id , True)
            else:
                modify_opt_result_db(campaign_id , False)
    return


# In[ ]:


if __name__=="__main__":
    start_time = datetime.datetime.now()
    print(start_time)
    optimize_performance_campaign()
    optimize_branding_campaign()
    print(datetime.datetime.now() - start_time)
#     main(1896291336)


# In[8]:


def retrieve_bid_modifier(client):
    ad_group_bid_modifier_service = adwords_client.GetService(
        'CampaignBidModifierService', version='v201809')
    # Get all ad group bid modifiers for the campaign.
    selector = {
        'fields': ['CampaignId', 'AdGroupId', 'BidModifier', 'Id', 'Name'],
        'predicates': [
            {
                'field': 'CamapaignId',
                'operator': 'EQUALS',
                'values': [1896291336]
            }
        ]
    }
    resp = ad_group_bid_modifier_service.get(selector)
    return resp


# In[ ]:


retrieve_bid_modefier(adwords_client)


# In[9]:


DEVICE_CRITERION = {
    'Desktop': 30000,
    'Mobile': 30001,
    'Tablet': 30002,
    'Tv': 30004,
}
def assign_bid_modifier(client, ad_group_id, bid_modifier, device='Desktop'):
    # Initialize appropriate service.
    ad_group_bid_modifier_service = adwords_client.GetService(
        'AdGroupBidModifierService', version='v201809')
    operation = {
        # Use 'ADD' to add a new modifier and 'SET' to update an existing one. A
        # modifier can be removed with the 'REMOVE' operator.
        'operator': 'ADD',
        'operand': {
            'adGroupId': ad_group_id,
            'criterion': {
                'xsi_type': 'Platform',
                'id': 30001
            },
            'bidModifier': bid_modifier
        }
    }
    response = ad_group_bid_modifier_service.mutate([operation])
    return response


# In[ ]:


assign_bid_modifier(adwords_client, 69785365626, 1.0, device='Desktop')


# In[10]:


CUSTOMER_ID = 5922380045
CAMPAIGN_ID = 1896291336
adwords_client.SetClientCustomerId(CUSTOMER_ID)


# In[11]:


def handle_initial_bids(campaign_id, spend, budget):
    if spend >= 1.5 * budget :
        print('[handle_initial_bids] stay_init_bid, campaign_id: ', campaign_id)
    elif spend < budget:
        print('[handle_initial_bids] adjust_init_bid, campaign_id: ', campaign_id)
        gdn_db.adjust_init_bid(campaign_id)
    return


# In[21]:


def optimize_performance_campaign():
    df_performance_campaign = gdn_db.get_performance_campaign_is_running()
    campaign_id_list = df_performance_campaign['campaign_id'].tolist()
    for campaign_id in campaign_id_list:
        customer_id = df_performance_campaign['customer_id'][df_performance_campaign.campaign_id==campaign_id].iloc[0]
        destination_type = df_performance_campaign['destination_type'][df_performance_campaign.campaign_id==campaign_id].iloc[0]
        daily_target = df_performance_campaign['daily_target'][df_performance_campaign.campaign_id==campaign_id].iloc[0]
        adwords_client.SetClientCustomerId( customer_id )
        target = 'conversions'
        # Init datacollector Campaign
        camp = Campaign(customer_id, campaign_id, destination_type)
        day_dict = camp.get_campaign_insights(adwords_client,
            date_preset=DatePreset.yesterday)
        lifetime_dict = camp.get_campaign_insights(adwords_client,
            date_preset=DatePreset.lifetime)
        target = int( day_dict[target] )
        achieving_rate = target / daily_target
        print('[campaign_id]', campaign_id, )
        print('[achieving rate]', achieving_rate, '[target]', target, '[daily_target]', daily_target)
        # Init param retriever Retrieve
        retriever = Retrieve(customer_id)
        retriever.generate_ad_group_id_list_type(campaign_id)
        if achieving_rate < 1 and achieving_rate >= 0:
            # Assign criterion to native ad group
            make_adgroup_with_criterion(adwords_client, update, campaign_id,
                                        native_ad_group_id_list=retriever.native_ad_group_id_list,)
            modify_opt_result_db(campaign_id , True)
        else:
            modify_opt_result_db(campaign_id , False)
    return


# In[22]:


optimize_performance_campaign()


# In[ ]:


def optimize_branding_campaign():
    df_branding_campaign = get_branding_campaign_is_running()
    campaign_id_list = df_branding_campaign['campaign_id'].tolist()
    for campaign_id in campaign_id_list:
        customer_id = df_branding_campaign['customer_id'][df_branding_campaign.campaign_id==campaign_id].iloc[0]
        destination_type = df_branding_campaign['destination_type'][df_branding_campaign.campaign_id==campaign_id].iloc[0]
        daily_target = df_branding_campaign['daily_target'][df_branding_campaign.campaign_id==campaign_id].iloc[0]
        adwords_client.SetClientCustomerId( customer_id )
        target = 'clicks'
        # Init datacollector Campaign
        camp = Campaign(customer_id, campaign_id, destination_type)
        day_dict = camp.get_campaign_insights(adwords_client,
            date_preset=DatePreset.yesterday)
        lifetime_dict = camp.get_campaign_insights(adwords_client,
            date_preset=DatePreset.lifetime)
        target = int( day_dict[target] )
        achieving_rate = target / daily_target
        print('[campaign_id]', campaign_id, )
        print('[achieving rate]', achieving_rate, '[target]', target, '[daily_target]', daily_target)
        # Init param retriever Retrieve
        retriever = Retrieve(customer_id)
        retriever.generate_ad_group_id_list_type(campaign_id)
        if achieving_rate < 1 and achieving_rate >= 0:
            update = Update(customer_id)
            # Make empty mutant ad group
            mutant_ad_group_id = make_empty_ad_group(adwords_client, campaign_id, retriever.native_ad_group_id_list)
            # Assign criterion to native ad group
            make_adgroup_with_criterion(adwords_client, update, campaign_id,
                                        native_ad_group_id_list=[mutant_ad_group_id],)

            modify_opt_result_db(campaign_id , True)
        else:
            modify_opt_result_db(campaign_id , False)
    return

