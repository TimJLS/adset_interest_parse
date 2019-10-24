#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import gdn_datacollector as collector
import gdn_gsn_ai_behavior_log as logger
from gdn_gsn_ai_behavior_log import BehaviorType
import datetime
import database_controller
from googleads import adwords
import pandas as pd
import copy
import math
IS_DEBUG = False
DATABASE = "dev_gdn"
DATE = datetime.datetime.now().date()
DATETIME = datetime.datetime.now()
AGE_RANGE_LIST = [503001,503002,503003,503004,503005,503006,503999,]

import google_adwords_controller as controller
import gdn_custom_audience as custom_audience


# In[ ]:


class Index:
    criteria_column = {
#         'URL': 'url_display_name',
        'AUDIENCE': 'criterion_id',
        'DISPLAY_KEYWORD': 'keyword_id',
        'DISPLAY_TOPICS': 'criterion_id',
    }
    criteria_type = {   
#         'URL': 'Placement',
        'AUDIENCE': 'CriterionUserInterest',
        'DISPLAY_KEYWORD': 'Keyword',
        'DISPLAY_TOPICS': 'Vertical',
    }
    score = {  
#         'URL': 'url',
#         'CRITERIA': 'criteria',
        'AUDIENCE': 'audience',
        'DISPLAY_KEYWORD': 'display_keyword',
        'DISPLAY_TOPICS': 'display_topics',
    }


# In[ ]:


def modify_opt_result_db(campaign_id, is_optimized):
    #get date
    opt_date = datetime.date.today()
    database_gdn.update("campaign_target", {"is_optimized": is_optimized, "optimized_date": opt_date }, campaign_id=campaign_id)


# In[ ]:


def make_criterion(new_ad_group_id, df, criteria):
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
                criterion['id'] = criterion_id
                criterion['matchType'] = 'BROAD'
                criterion['text'] = df['keyword'].iloc[i]
                
            elif Index.criteria_type[criteria] == 'CriterionUserInterest':
                criterion['id'] = criterion_id
                criterion['userInterestId'] = df['audience'].iloc[i]
                
            elif Index.criteria_type[criteria] == 'CriterionUserList':
                criterion['id'] = criterion_id
                criterion['userListId'] = df['audience'].iloc[i]
                
            elif Index.criteria_type[criteria] == 'CriterionCustomAffinity':
                criterion['id'] = criterion_id
                criterion['customAffinityId'] = df['audience'].iloc[i]
                
            elif Index.criteria_type[criteria] == 'Vertical':
                criterion['id'] = criterion_id
                criterion['verticalId'] = df['vertical_id'].iloc[i]
                
            sub_criterions.append(copy.deepcopy(criterion))
    return sub_criterions


# In[ ]:


def make_audience_criterion_by_score(campaign_id, new_ad_group_id,):
    biddable_criterions = []
    negative_criterions = []
    biddable_sub_criterion = []
    negative_sub_criterion = []
    # select score by campaign level
    df = database_gdn.retrieve('audience_score', campaign_id)

    df['request_time'] = pd.to_datetime(df['request_time'])
    df = df[ df.request_time.dt.date == (datetime.datetime.now().date()) ]
    if not df.empty:
        df['audience_type'] = df.audience.str.split('::', expand=True)[0]
        df['audience'] = df.audience.str.split('::', expand=True)[1]
        df = df[~df.audience_type.isin(['boomuserlist'])]
        df = df[~df.audience_type.isin(['custominmarket'])]

        df = df[df.adgroup_id == new_ad_group_id]
        df = df.sort_values(by=['score'], ascending=False).drop_duplicates(
            [ 'criterion_id' ] ).reset_index(drop=True)
        half = math.ceil( len(df)/2 )
        biddable_df = df.iloc[:half]
        negative_df = df.iloc[half:]
        biddable_sub_criterion = make_criterion(new_ad_group_id, biddable_df, 'AUDIENCE')
        negative_sub_criterion = make_criterion(new_ad_group_id, negative_df, 'AUDIENCE')
        biddable_criterions += biddable_sub_criterion
        negative_criterions += negative_sub_criterion
    return biddable_criterions, negative_criterions


# In[ ]:


def make_topic_criterion_by_score(campaign_id, new_ad_group_id,):
    criteria = 'DISPLAY_TOPICS'
    biddable_criterions = []
    negative_criterions = []
    biddable_sub_criterion = []
    negative_sub_criterion = []
    # select score by campaign level
    df = database_gdn.retrieve('display_topics_score', campaign_id)

    df['request_time'] = pd.to_datetime(df['request_time'])
    df = df[ df.request_time.dt.date == (datetime.datetime.now().date()) ]
    if not df.empty:

        df = df[df.adgroup_id == new_ad_group_id]
        df = df.sort_values(by=['score'], ascending=False).drop_duplicates(
            [ 'criterion_id' ] ).reset_index(drop=True)
        half = math.ceil( len(df)/2 )
        biddable_df = df.iloc[:half]
        negative_df = df.iloc[half:]
        biddable_sub_criterion = make_criterion(new_ad_group_id, biddable_df, 'DISPLAY_TOPICS')
        negative_sub_criterion = make_criterion(new_ad_group_id, negative_df, 'DISPLAY_TOPICS')
        biddable_criterions += biddable_sub_criterion
        negative_criterions += negative_sub_criterion
    return biddable_criterions, negative_criterions


# In[ ]:


def make_keyword_criterion_by_score(campaign_id, new_ad_group_id):
    criteria = 'DISPLAY_KEYWORD'
    biddable_criterions = []
    negative_criterions = []
    biddable_sub_criterion = []
    negative_sub_criterion = []
    # select score by campaign level
    df = database_gdn.retrieve('display_keyword_score', campaign_id)
    df['request_time'] = pd.to_datetime(df['request_time'])
    df = df[ df.request_time.dt.date == (datetime.datetime.now().date()) ]
    if not df.empty:
        df = df[df.adgroup_id == new_ad_group_id]
        df = df.sort_values(by=['score'], ascending=False).reset_index(drop=True)
        half = math.ceil( len(df)/2 )
        biddable_df = df.iloc[:half]
        negative_df = df.iloc[half:]
        biddable_sub_criterion = make_criterion(new_ad_group_id, biddable_df, 'DISPLAY_KEYWORD')
        negative_sub_criterion = make_criterion(new_ad_group_id, negative_df, 'DISPLAY_KEYWORD')
        biddable_criterions += biddable_sub_criterion
        negative_criterions += negative_sub_criterion
    return biddable_criterions, negative_criterions


# In[ ]:


def make_empty_ad_group(service_container, campaign_id, ad_group):
    
    new_ad_group = service_container.make_ad_group(ad_group)
    
    ad_group_pair = {
        'db_type': 'dev_gdn', 'campaign_id': campaign_id,
        'adgroup_id': new_ad_group.ad_group_id, 'criterion_id': new_ad_group.ad_group_id, 'criterion_type': 'adgroup'
    }
    logger.save_adgroup_behavior(BehaviorType.CREATE, **ad_group_pair)
    return new_ad_group


# In[ ]:


def is_assessed(campaign_id):
# Check if assessment is done
    is_assessed = False
    for criteria in Index.score.keys():
        df = database_gdn.retrieve(Index.score[criteria]+"_score", campaign_id)
        df['request_time'] = pd.to_datetime(df['request_time'])
        df = df[ df.request_time.dt.date == (datetime.datetime.now().date()) ]
        if not df.empty:
            is_assessed = True
    if not is_assessed:
        print('[make_adgroup_with_criterion]: campaign_id {} is not assessed.'.format(campaign_id))
    return is_assessed


# In[ ]:


def handle_initial_bids(ad_group_id, spend, budget, daily_target, original_cpa):
    if IS_DEBUG:
        print('[handle_initial_bids] IS_DEBUG == True , not adjust bid')
        return
    
    if daily_target < 0:
        if database_gdn.get_init_bid(ad_group_id) >= original_cpa:
            print('[handle_initial_bids] good enough , lower the bid')
            database_gdn.update_init_bid(adset_id=ad_group_id, update_ratio=0.9)
        else:
            print('[handle_initial_bids] good enough , keep the bid', ', original_cpa:', original_cpa)
    elif spend <= budget * 0.8:
        print('[handle_initial_bids] adjust_init_bid up the bid)')
        database_gdn.update_init_bid(adset_id=ad_group_id, update_ratio=1.1)
    else:
        print('[handle_initial_bids] stay_init_bid')


# In[ ]:


def make_basic_criterion(native_ad_group, mutant_ad_group):
    biddable, negative = native_ad_group.basic_criterions.retrieve()
#     try:
    mutant_ad_group.basic_criterions.update(biddable, is_delivering=True, is_included=True)
#     except Exception as e:
#         print('[make_basic_adgroup_criterion]: biddable basic criterion update failed.')
#         print(e)
    try:
        mutant_ad_group.basic_criterions.update(negative, is_delivering=False, is_included=False)
    except Exception as e:
        print('[make_basic_adgroup_criterion]: negative basic criterion already set')
        print(e)


# In[ ]:


def make_user_interest_criterion(service_container, campaign_id, native_ad_group, mutant_ad_group=None):
    native_id = native_ad_group.ad_group_id
    if mutant_ad_group: 
        mutant_id = mutant_ad_group.ad_group_id
    else:
        mutant_id = native_ad_group.ad_group_id
    # Criterion by Score
    print('[mutant_id] ', mutant_id)
    biddable_criterions, negative_criterions = make_audience_criterion_by_score( campaign_id, mutant_id )
    biddable_criterions = [ biddable_criterion for biddable_criterion in biddable_criterions if biddable_criterion.get("xsi_type") == 'CriterionUserInterest' ]
    negative_criterions = [ negative_criterion for negative_criterion in negative_criterions if negative_criterion.get("xsi_type") == 'CriterionUserInterest' ]
    print('[biddable_criterions]: ', biddable_criterions)
    print('[negative_criterions]: ', negative_criterions)
    
    ad_group = controller.AdGroup(service_container, ad_group_id=mutant_id)
    for biddable_criterion in biddable_criterions:
#         try:
        ad_group.user_interest_criterions.update([biddable_criterion], is_delivering=True, is_included=True)
        audience_pair = {
            'db_type': 'dev_gdn',
            'campaign_id': campaign_id,
            'adgroup_id': mutant_id,
            'criterion_id': biddable_criterion['id'],
            'criterion_type': 'audience'
        }
        logger.save_adgroup_behavior(BehaviorType.OPEN, **audience_pair)
#         except Exception as e:
#             print('!!!!!!!!!!![make_adgroup_with_criterion]: update biddable user_interest criterion failed. criterion id: ', biddable_criterion['id'])
#             print(e)
#             pass
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
            print('!!!!!!!!!!![make_adgroup_with_criterion]: update negative user_interest criterion failed. criterion id: ', negative_criterion['id'])
            print(e)
            pass
    # Assign Ad Params

    native_ad_list = native_ad_group.get_ads()
    if native_id == mutant_id:
        return
    for native_ad in native_ad_list:
        try:
            result = native_ad.assign(mutant_id)
        except Exception as e:
            print('[assign ad to ad_group]: action assign failed., ', e)
            pass
    return


# In[ ]:


def make_display_keyword_criterion(campaign_id, controller_ad_group):
    biddable_criterions, negative_criterions = make_display_keyword_criterion( campaign_id, controller_ad_group.ad_group_id )
    biddable_criterions = [ biddable_criterion for biddable_criterion in biddable_criterions if biddable_criterion.get("xsi_type") == 'CriterionUserInterest' ]
    negative_criterions = [ negative_criterion for negative_criterion in negative_criterions if negative_criterion.get("xsi_type") == 'CriterionUserInterest' ]
    print('[biddable_criterions]: ', biddable_criterions)
    print('[negative_criterions]: ', negative_criterions)
    keywords = controller_ad_group.get_keywords()
    keywords = [keyword.retrieve() for keyword in keywords]
    biddable_keywords = [keyword.update_status('ENABLED') for keyword in keywords if keyword.keyword_dict['keyword_id'] in [biddable_criterion['id'] for biddable_criterion in biddable_criterions]]
    negative_keywords = [keyword.update_status('PAUSED') for keyword in keywords if keyword.keyword_dict['keyword_id'] in [negative_criterion['id'] for negative_criterion in negative_criterions]]
    for biddable_keyword in biddable_keywords:
        audience_pair = {
            'db_type': 'dev_gdn',
            'campaign_id': campaign_id,
            'adgroup_id': controller_ad_group.ad_group_id,
            'criterion_id': biddable_keyword.keyword_dict['id'],
            'criterion_type': 'display_keyword'
        }
        logger.save_adgroup_behavior(BehaviorType.OPEN, **audience_pair)
    for negative_keyword in negative_keywords:
        audience_pair = {
            'db_type': 'dev_gdn',
            'campaign_id': campaign_id,
            'adgroup_id': controller_ad_group.ad_group_id,
            'criterion_id': negative_keyword.keyword_dict['id'],
            'criterion_type': 'display_keyword'
        }
        logger.save_adgroup_behavior(BehaviorType.OPEN, **audience_pair)
    return


# In[ ]:


def make_display_topics_criterion(campaign_id, controller_ad_group):
    biddable_criterions, negative_criterions = make_topic_criterion_by_score( campaign_id, controller_ad_group.ad_group_id )
    biddable_criterions = [ biddable_criterion for biddable_criterion in biddable_criterions if biddable_criterion.get("xsi_type") == 'CriterionUserInterest' ]
    negative_criterions = [ negative_criterion for negative_criterion in negative_criterions if negative_criterion.get("xsi_type") == 'CriterionUserInterest' ]
    print('[biddable_criterions]: ', biddable_criterions)
    print('[negative_criterions]: ', negative_criterions)
    for biddable_criterion in biddable_criterions:
#         try:
        ad_group.user_vertical_criterions.update([biddable_criterion], is_delivering=True, is_included=True)
        audience_pair = {
            'db_type': 'dev_gdn',
            'campaign_id': campaign_id,
            'adgroup_id': controller_ad_group.ad_group_id,
            'criterion_id': biddable_criterion['id'],
            'criterion_type': 'display_topics'
        }
        logger.save_adgroup_behavior(BehaviorType.OPEN, **audience_pair)
    for negative_criterion in negative_criterions:
#         try:
        ad_group.user_vertical_criterions.update([negative_criterion], is_delivering=False, is_included=True)
        audience_pair = {
            'db_type': 'dev_gdn',
            'campaign_id': campaign_id,
            'adgroup_id': controller_ad_group.ad_group_id,
            'criterion_id': negative_criterion['id'],
            'criterion_type': 'display_topics'
        }
        logger.save_adgroup_behavior(BehaviorType.CLOSE, **audience_pair)
    return


# In[ ]:


def make_user_list_criterion(campaign_id, ad_group):
    optimized_list_dict_list, all_converters_dict_list = custom_audience.get_campaign_custom_audience(campaign_id)
    ad_group_id_list = []
    for user_list in optimized_list_dict_list:
        try:
            ad_group.user_list_criterions.make(user_list_id=user_list['criterion_id'])
            custom_audience.modify_result_db(campaign_id, user_list['criterion_id'], ad_group.ad_group_id)
        except Exception as e:
            print('!!!!!!!!!!![make_user_list_criterion]: update optimized_list criterion failed. user_list id ', user_list['criterion_id'])
            pass

    for user_list in all_converters_dict_list:
#         try:
        ad_group.user_list_criterions.make(user_list_id=user_list['criterion_id'])
        custom_audience.modify_result_db(campaign_id, user_list['criterion_id'], ad_group.ad_group_id)
#         except Exception as e:
#             print('!!!!!!!!!!![make_user_list_criterion]: update all_converters criterion failed. user_list id ', criterion['criterion_id'])
#             pass
    


# In[ ]:


def optimize_performance_campaign():
    performance_campaign_dict_list = database_gdn.get_performance_campaign().to_dict('records')
    campaign_id_list = [ performance_campaign_dict['campaign_id'] for performance_campaign_dict in performance_campaign_dict_list ]
    print('[optimize_performance_campaign]: campaign_id_list', campaign_id_list)
    for performance_campaign_dict in performance_campaign_dict_list:
        customer_id = performance_campaign_dict['customer_id']
        campaign_id = performance_campaign_dict['campaign_id']
        destination_type = performance_campaign_dict['destination_type']
        daily_target = performance_campaign_dict['daily_target']
        is_lookalike = eval(performance_campaign_dict['is_lookalike'])
        destination = performance_campaign_dict['destination']
        ai_spend_cap = performance_campaign_dict['ai_spend_cap']
        original_cpa = ai_spend_cap/destination
        print('[optimize_performance_campaign] campaign_id:' , campaign_id)
        print('[optimize_performance_campaign] original_cpa:' , original_cpa)
        
        service_container = controller.AdGroupServiceContainer( customer_id )
        
        objective = 'conversions'
        # Init datacollector Campaign
        collector_campaign = collector.Campaign(customer_id, campaign_id, destination_type)
        day_dict = collector_campaign.get_campaign_insights(date_preset=collector.DatePreset.yesterday)
        lifetime_dict = collector_campaign.get_campaign_insights(date_preset=collector.DatePreset.lifetime)
        # Init param retriever Retrieve
        controller_campaign = controller.Campaign(service_container, campaign_id)
        controller_campaign.generate_ad_group_id_type_list()
        native_ad_group_id_list = controller_campaign.native_ad_group_id_list
        # Adjust initial bids
        for ad_group_id in native_ad_group_id_list:
            handle_initial_bids(ad_group_id, day_dict['spend'], day_dict['daily_budget'], daily_target, original_cpa)
        
        target = int( day_dict[objective] )
        achieving_rate = (target / daily_target) if daily_target != 0 else 0
        print('[optimize_performance_campaign][achieving rate]', achieving_rate, '[target]', target, '[daily_target]', daily_target)

        
        if is_assessed(campaign_id):
            print('[optimize_branding_campaign]: campaign is assessed.')
            # Assign criterion to native ad group
            for native_id in native_ad_group_id_list:
                native_ad_group = controller.AdGroup(service_container, ad_group_id=native_id)
                make_user_interest_criterion(
                    service_container, campaign_id, native_ad_group,)
                if is_lookalike:
                    make_user_list_criterion(campaign_id, native_ad_group)
            modify_opt_result_db(campaign_id , True)
        else:
            print('[optimize_branding_campaign] campaign is not assessed. campaign_id: ', campaign_id)
            modify_opt_result_db(campaign_id , False)


# In[ ]:


def optimize_branding_campaign():
    branding_campaign_dict_list = database_gdn.get_branding_campaign().to_dict('records')
    campaign_id_list = [ branding_campaign_dict['campaign_id'] for branding_campaign_dict in branding_campaign_dict_list ]
    print('[optimize_branding_campaign]: campaign_id_list', campaign_id_list)
    
    for branding_campaign_dict in branding_campaign_dict_list:
        customer_id = branding_campaign_dict['customer_id']
        campaign_id = branding_campaign_dict['campaign_id']
        destination_type = branding_campaign_dict['destination_type']
        daily_target = branding_campaign_dict['daily_target']
        is_lookalike = eval(branding_campaign_dict['is_lookalike'])
        destination = branding_campaign_dict['destination']
        ai_spend_cap = branding_campaign_dict['ai_spend_cap']
        original_cpc = ai_spend_cap/destination
        print('[optimize_branding_campaign] campaign_id', campaign_id)
        print('[optimize_branding_campaign] original_cpc:' , original_cpc)

        service_container = controller.AdGroupServiceContainer( customer_id )
        
        objective = 'clicks'
        # Init datacollector Campaign
        collector_campaign = collector.Campaign(customer_id, campaign_id, destination_type)
        day_dict = collector_campaign.get_campaign_insights(date_preset=collector.DatePreset.yesterday)
        print('[optimize_branding_campaign] day_dict', day_dict)
        lifetime_dict = collector_campaign.get_campaign_insights(date_preset=collector.DatePreset.lifetime)
        print('[optimize_branding_campaign] lifetime_dict', lifetime_dict)
        # Init param retriever Retrieve
        controller_campaign = controller.Campaign(service_container, campaign_id)
        controller_campaign.generate_ad_group_id_type_list()
        native_ad_group_id_list = controller_campaign.native_ad_group_id_list
        mutate_ad_group_id_list = controller_campaign.mutate_ad_group_id_list        
        # Adjust initial bids
        for ad_group_id in native_ad_group_id_list:
            handle_initial_bids(ad_group_id, day_dict['spend'], day_dict['daily_budget'], daily_target, original_cpc)
        target = int( day_dict[objective] )
        achieving_rate = (target / daily_target) if daily_target != 0 else 0
        print('[optimize_branding_campaign][achieving rate]', achieving_rate, '[target]', target, '[daily_target]', daily_target)


        if achieving_rate < 1 and achieving_rate >= 0:
            # Pause all on-line mutant
            for ad_group_id in mutate_ad_group_id_list:
                ad_group = controller.AdGroup(service_container, ad_group_id=ad_group_id)
                ad_group.param.update_status(status=controller.Status.pause)
            if is_assessed(campaign_id):
                print('[optimize_branding_campaign]: campaign is assessed.')
                for native_ad_group_id in native_ad_group_id_list:
                    native_ad_group = controller.AdGroup(service_container, ad_group_id=native_ad_group_id)
                    # Make empty mutant ad group
    #                 mutant_ad_group = make_empty_ad_group(service_container, campaign_id, native_ad_group)
    #                 make_basic_criterion( native_ad_group = native_ad_group, mutant_ad_group = mutant_ad_group )
                    # Assign criterion to native ad group
                    make_user_interest_criterion(
                        service_container, campaign_id,
                        native_ad_group = native_ad_group,
                        mutant_ad_group = native_ad_group
                    )
                    if is_lookalike:
                        make_user_list_criterion(campaign_id, native_ad_group)
                        
                modify_opt_result_db(campaign_id , True)
            else:
                print('[optimize_branding_campaign] campaign is not assessed. campaign_id: ', campaign_id)
                modify_opt_result_db(campaign_id , False)
        else:
            print('[optimize_branding_campaign] spend money normal , achieving_rate is good ', achieving_rate)
            modify_opt_result_db(campaign_id , False)
        print('[optimize_branding_campaign]: next campaign====================')
    print('[gdn_externals]: main finish!!!!!!!!====================')


# In[ ]:


if __name__=="__main__":
    start_time = datetime.datetime.now()
    print('current time: ', start_time)
    global database_fb
    db = database_controller.Database()
    database_gdn = database_controller.GDN(db)
    optimize_performance_campaign()
    optimize_branding_campaign()
    print(datetime.datetime.now() - start_time)


# In[ ]:


# !jupyter nbconvert --to script gdn_externals.ipynb


# In[ ]:


# customer_id = 6714857152
# campaign_id = 2053556135


# In[ ]:


# optimize_performance_campaign()


# In[ ]:


# optimize_branding_campaign()


# In[ ]:


# pos, neg = make_keyword_criterion_by_score(6491023637, 77398403505)
# make_topic_criterion_by_score(2080506438, 78846749233)

