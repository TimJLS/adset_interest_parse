#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gdn_datacollector as collector
import gdn_gsn_ai_behavior_log as logger
from gdn_gsn_ai_behavior_log import BehaviorType
import datetime
import gdn_db
from googleads import adwords
import pandas as pd
import copy
import math
IS_DEBUG = False
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
DATABASE = "dev_gdn"
DATE = datetime.datetime.now().date()
DATETIME = datetime.datetime.now()
AGE_RANGE_LIST = [503001,503002,503003,503004,503005,503006,503999,]

import google_adwords_controller as controller
import gdn_custom_audience as custom_audience


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


def make_criterion(new_ad_group_id, df,criteria):
    sub_criterions = []
    # make criterion stucture in order to upadate back to gdn platform
    criterion = {
        'xsi_type': None
    }
    if criteria == 'AGE_RANGE':
        criterion['xsi_type'] = Index.criteria_type[criteria]
        for i, criterion_id in enumerate(AGE_RANGE_LIST):
            criterion['id'] = criterion_id
            sub_criterions.append(copy.deepcopy(criterion))
    else:
        criterion['xsi_type'] = Index.criteria_type[criteria]
        for i, criterion_id in enumerate(df[Index.criteria_column[criteria]]):
            if Index.criteria_type[criteria] == 'Placement':
                criterion['id'] = criterion_id
                criterion['url'] = df['url_display_name'].iloc[i]
                
            elif Index.criteria_type[criteria] == 'Keyword':
                criterion['matchType'] = 'BROAD'
                criterion['text'] = df['keyword'].iloc[i]
                
            elif Index.criteria_type[criteria] == 'CriterionUserInterest':
                criterion['userInterestId'] = df['audience'].iloc[i]
                criterion['id'] = criterion_id
                
            elif Index.criteria_type[criteria] == 'CriterionUserList':
                criterion['id'] = criterion_id
                criterion['userListId'] = df['audience'].iloc[i]
                
            elif Index.criteria_type[criteria] == 'CriterionCustomAffinity':
                criterion['id'] = criterion_id
                criterion['customAffinityId'] = df['audience'].iloc[i]
                
            sub_criterions.append(copy.deepcopy(criterion))
    return sub_criterions

def make_adgroup_criterion_by_score(campaign_id, new_ad_group_id,):
    biddable_criterions = []
    negative_criterions = []
    biddable_sub_criterion = []
    negative_sub_criterion = []

    for criteria in Index.score.keys():
        # select score by campaign level
        df = gdn_db.get_table(campaign_id=campaign_id,
                              table=Index.score[criteria]+"_score")
        df['request_time'] = pd.to_datetime(df['request_time'])
        df = df[ df.request_time.dt.date == (datetime.datetime.now().date()) ]
        if criteria == 'AUDIENCE' and not df.empty:
            df['audience_type'] = df.audience.str.split('::', expand=True)[0]
            df['audience'] = df.audience.str.split('::', expand=True)[1]
            df = df[~df.audience_type.isin(['boomuserlist'])]
        if not df.empty:
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

def make_empty_ad_group(service_container, campaign_id, ad_group):
    
    new_ad_group = service_container.make_ad_group(ad_group)
    
    ad_group_pair = {
        'db_type': 'dev_gdn', 'campaign_id': campaign_id,
        'adgroup_id': new_ad_group.ad_group_id, 'criterion_id': new_ad_group.ad_group_id, 'criterion_type': 'adgroup'
    }
    logger.save_adgroup_behavior(BehaviorType.CREATE, **ad_group_pair)
    return new_ad_group


# In[4]:


def is_assessed(campaign_id):
# Check if assessment is done
    is_assessed = False
    for criteria in Index.score.keys():
        df = gdn_db.get_table( campaign_id=campaign_id, table=Index.score[criteria]+"_score" )
        df['request_time'] = pd.to_datetime(df['request_time'])
        df = df[ df.request_time.dt.date == (datetime.datetime.now().date()) ]
        if not df.empty:
            is_assessed = True
    if not is_assessed:
        print('[make_adgroup_with_criterion]: campaign_id {} is not assessed.'.format(campaign_id))
    return is_assessed


# In[5]:


def handle_initial_bids(campaign_id, spend, budget, daily_target, original_cpa):
    if IS_DEBUG:
        print('[handle_initial_bids] IS_DEBUG == True , not adjust bid')
        return
    
    if daily_target < 0:
        if gdn_db.get_current_init_bid(campaign_id) >= original_cpa:
            print('[handle_initial_bids] good enough , lower the bid')
            gdn_db.adjust_init_bid(campaign_id, 0.9)
        else:
            print('[handle_initial_bids] good enough , keep the bid', ', original_cpa:', original_cpa)
    elif spend <= budget * 0.8:
        print('[handle_initial_bids] adjust_init_bid up the bid)')
        gdn_db.adjust_init_bid(campaign_id, 1.1)
    else:
        print('[handle_initial_bids] stay_init_bid')


# In[6]:


def make_basic_criterion(native_ad_group, mutant_ad_group):
    biddable, negative = native_ad_group.basic_criterions.retrieve()
    try:
        mutant_ad_group.basic_criterions.update(biddable, is_delivering=True, is_included=True)
    except Exception as e:
        print('[make_basic_adgroup_criterion]: biddable basic criterion update failed.')
        print(e)
    try:
        mutant_ad_group.basic_criterions.update(negative, is_delivering=False, is_included=False)
    except Exception as e:
        print('[make_basic_adgroup_criterion]: negative basic criterion already set')
        print(e)


# In[7]:


def make_user_interest_criterion(sevice_container, campaign_id, native_ad_group, mutant_ad_group=None):
    native_id = native_ad_group.ad_group_id
    if mutant_ad_group: 
        mutant_id = mutant_ad_group.ad_group_id
    else:
        mutant_id = native_ad_group.ad_group_id
    # Criterion by Score
    print('[mutant_id] ', mutant_id)
    biddable_criterions, negative_criterions = make_adgroup_criterion_by_score( campaign_id, mutant_id )
    print('[biddable_criterions]: ', biddable_criterions)
    print('[negative_criterions]: ', negative_criterions)
    
    ad_group = controller.AdGroup(service_container, ad_group_id=mutant_id)
    for biddable_criterion in biddable_criterions:
        try:
            ad_group.user_interest_criterions.update([biddable_criterion], is_delivering=True, is_included=True)
            audience_pair = {
                'db_type': 'dev_gdn',
                'campaign_id': campaign_id,
                'adgroup_id': mutant_id,
                'criterion_id': biddable_criterion['id'],
                'criterion_type': 'audience'
            }
            logger.save_adgroup_behavior(BehaviorType.OPEN, **audience_pair)
        except Exception as e:
            print('[make_adgroup_with_criterion]: update biddable user_interest criterion failed. criterion id: ', biddable_criterion['id'])
            print(e)
            pass
    for negative_criterion in negative_criterions:
        try:
            ad_group.user_interest_criterions.update([negative_criterion], is_delivering=False, is_included=True)
            audience_pair = {
                'db_type': 'dev_gdn',
                'campaign_id': campaign_id,
                'adgroup_id': mutant_id,
                'criterion_id': negative_criterion['id'],
                'criterion_type': 'audience'
            }
            logger.save_adgroup_behavior(BehaviorType.CLOSE, **audience_pair)
        except Exception as e:
            print('[make_adgroup_with_criterion]: update negative user_interest criterion failed. criterion id: ', biddable_criterion['id'])
            print(e)
            pass
    # Assign Ad Params

    native_ad_list = native_ad_group.get_ads()

    for native_ad in native_ad_list:
        try:
            result = native_ad.assign(mutant_id)
        except Exception as e:
            print('[assign ad to ad_group]: action assign failed., ', e)
            pass
    return


# In[8]:


def make_user_list_criterion(campaign_id, ad_group):
    optimized_list_dict_list, all_converters_dict_list = custom_audience.get_campaign_custom_audience(campaign_id)
    ad_group_id_list = []
    for criterion in optimized_list_dict_list:
        try:
            ad_group.user_list_criterions.make(criterion_id=criterion['criterion_id'])
            custom_audience.modify_result_db(campaign_id, criterion['criterion_id'], ad_group.ad_group_id)
        except Exception as e:
            print('[make_user_list_criterion]: update optimized_list criterion failed. user_list id ', criterion['criterion_id'])
            pass

    for criterion in all_converters_dict_list:
        try:
            ad_group.user_list_criterions.make(criterion_id=criterion['criterion_id'])
            custom_audience.modify_result_db(campaign_id, criterion['criterion_id'], ad_group.ad_group_id)
        except Exception as e:
            print('[make_user_list_criterion]: update all_converters criterion failed. user_list id ', criterion['criterion_id'])
            pass
    


# In[9]:


def optimize_performance_campaign():
    performance_campaign_dict_list = gdn_db.get_performance_campaign_is_running().to_dict('records')
    campaign_id_list = [ performance_campaign_dict['campaign_id'] for performance_campaign_dict in performance_campaign_dict_list ]
    print('[optimize_performance_campaign]: campaign_id_list', campaign_id_list)
    for performance_campaign_dict in performance_campaign_dict_list:
        customer_id = performance_campaign_dict['customer_id']
        campaign_id = performance_campaign_dict['campaign_id']
        destination_type = performance_campaign_dict['destination_type']
        daily_target = performance_campaign_dict['daily_target']
        
        destination = performance_campaign_dict['destination']
        ai_spend_cap = performance_campaign_dict['ai_spend_cap']
        original_cpa = ai_spend_cap/destination
        print('[optimize_branding_campaign] campaign_id:' , campaign_id)
        print('[optimize_branding_campaign] original_cpa:' , original_cpa)
        
        adwords_client.SetClientCustomerId( customer_id )
        service_container = container.AdGroupServiceContainer( customer_id )
        
        objective = 'conversions'
        # Init datacollector Campaign
        collector_campaign = collector.Campaign(customer_id, campaign_id, destination_type)
        day_dict = collector_campaign.get_campaign_insights(
            adwords_client, date_preset=collector.DatePreset.yesterday)
        lifetime_dict = collector_campaign.get_campaign_insights(
            adwords_client, date_preset=collector.DatePreset.lifetime)
        # Adjust initial bids
        handle_initial_bids(campaign_id, day_dict['spend'], day_dict['daily_budget'], daily_target, original_cpa)
        
        target = int( day_dict[objective] )
        achieving_rate = target / daily_target
        print('[optimize_performance_campaign][achieving rate]', achieving_rate, '[target]', target, '[daily_target]', daily_target)
        # Init param retriever Retrieve
        controller_campaign = controller.Campaign(service_container, campaign_id)
        controller_campaign.generate_ad_group_id_type_list()
        native_ad_group_id_list = controller_campaign.native_ad_group_id_list
        native_ad_group_id = controller_campaign.native_ad_group_id_list[0]
        
        if is_assessed(campaign_id):
            print('[optimize_branding_campaign]: campaign is assessed.')
            # Assign criterion to native ad group
            for native_id in native_ad_group_id_list:
                native_ad_group = controller.AdGroup(service_container, ad_group_id=native_id)
                make_user_interest_criterion(
                    service_container, campaign_id,
                    ad_group=native_ad_group,)
                make_user_list_criterion(campaign_id, native_ad_group)
            modify_opt_result_db(campaign_id , True)

        else:
            print('[optimize_branding_campaign] campaign is not assessed. campaign_id: ', campaign_id)
            modify_opt_result_db(campaign_id , False)


# In[10]:


def optimize_branding_campaign():
    branding_campaign_dict_list = gdn_db.get_branding_campaign_is_running().to_dict('records')
    campaign_id_list = [ branding_campaign_dict['campaign_id'] for branding_campaign_dict in branding_campaign_dict_list ]
    print('[optimize_branding_campaign]: campaign_id_list', campaign_id_list)
    
    for branding_campaign_dict in branding_campaign_dict_list:
        customer_id = branding_campaign_dict['customer_id']
        campaign_id = branding_campaign_dict['campaign_id']
        destination_type = branding_campaign_dict['destination_type']
        daily_target = branding_campaign_dict['daily_target']
        
        destination = branding_campaign_dict['destination']
        ai_spend_cap = branding_campaign_dict['ai_spend_cap']
        original_cpc = ai_spend_cap/destination
        print('[optimize_branding_campaign] campaign_id', campaign_id)
        print('[optimize_branding_campaign] original_cpc:' , original_cpc)

        adwords_client.SetClientCustomerId( customer_id )
        service_container = controller.AdGroupServiceContainer( customer_id )
        
        objective = 'clicks'
        # Init datacollector Campaign
        collector_campaign = collector.Campaign(customer_id, campaign_id, destination_type)
        day_dict = collector_campaign.get_campaign_insights(adwords_client, date_preset=collector.DatePreset.yesterday)
        print('[optimize_branding_campaign] day_dict', day_dict)
        lifetime_dict = collector_campaign.get_campaign_insights(adwords_client, date_preset=collector.DatePreset.lifetime)
        print('[optimize_branding_campaign] lifetime_dict', lifetime_dict)
        
        # Adjust initial bids
        handle_initial_bids(campaign_id, day_dict['spend'], day_dict['daily_budget'], daily_target, original_cpc)
        target = int( day_dict[objective] )
        achieving_rate = target / daily_target
        print('[optimize_branding_campaign][achieving rate]', achieving_rate, '[target]', target, '[daily_target]', daily_target)
        # Init param retriever Retrieve
        controller_campaign = controller.Campaign(service_container, campaign_id)
        controller_campaign.generate_ad_group_id_type_list()
        native_ad_group_id_list = controller_campaign.native_ad_group_id_list
        native_ad_group_id = controller_campaign.native_ad_group_id_list[0]

        if achieving_rate < 1 and achieving_rate >= 0:
            update = Update(customer_id)
            # Pause all on-line mutant
            for ad_group_id in retriever.mutant_ad_group_id_list:
                ad_group = controller.AdGroup(service_container, ad_group_id=ad_group_id)
                ad_group.param.update_status(status=controller.Status.enable)
            if is_assessed(campaign_id):
                print('[optimize_branding_campaign]: campaign is assessed.')
                native_ad_group = controller.AdGroup(service_container, ad_group_id=native_ad_group_id)
                # Make empty mutant ad group
                mutant_ad_group = make_empty_ad_group(service_container, campaign_id, native_ad_group)
                
                make_basic_criterion( native_ad_group = native_ad_group, mutant_ad_group = mutant_ad_group )
                # Assign criterion to native ad group
                make_user_interest_criterion(
                    service_container, campaign_id,
                    native_ad_group = native_ad_group,
                    mutant_ad_group = mutant_ad_group
                )
                make_user_list_criterion(campaign_id, mutant_ad_group)
                
                modify_opt_result_db(campaign_id , True)
            else:
                print('[optimize_branding_campaign] campaign is not assessed. campaign_id: ', campaign_id)
                modify_opt_result_db(campaign_id , False)
        else:
            print('[optimize_branding_campaign] spend money normal , achieving_rate is good ', achieving_rate)
            modify_opt_result_db(campaign_id , False)
        print('[optimize_branding_campaign]: next campaign====================')
    print('[gdn_externals]: main finish!!!!!!!!====================')


# In[11]:


if __name__=="__main__":
    start_time = datetime.datetime.now()
    print('current time: ', start_time)
    optimize_performance_campaign()
    optimize_branding_campaign()
    print(datetime.datetime.now() - start_time)


# In[12]:


#!jupyter nbconvert --to script gdn_externals.ipynb


# In[ ]:


# customer_id = 6714857152
# campaign_id = 2053556135


# In[ ]:




