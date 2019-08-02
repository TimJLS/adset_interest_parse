#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
import datetime
import time

# from facebook_business.api import FacebookAdsApi
# from facebook_business.adobjects.adaccount import AdAccount
import facebook_business.adobjects.adset as facebook_business_adset
# from facebook_business.adobjects.ad import Ad
import facebook_business.adobjects.campaign as facebook_business_campaign
# from facebook_business.adobjects.adcreative import AdCreative
# from facebook_business.adobjects.adactivity import AdActivity
# from facebook_business.adobjects.insightsresult import InsightsResult
import facebook_business.adobjects.adsinsights as facebook_business_adsinsights

import facebook_datacollector as fb_collector
import mysql_adactivity_save as mysql_saver

IGNORE_ADSET_STR_LIST = ['AI', 'Copy', 'COPY', 'Lookalike', 'RT', 'Look-a-like']


def get_adset_name(adset_id):
    this_adset = facebook_business_adset.AdSet( adset_id).remote_read(fields=[ "name" ])
    return this_adset.get('name')

def get_account_id_by_campaign(campaign_id):
    this_campaign = facebook_business_campaign.Campaign( campaign_id ).remote_read(fields=["account_id"])
    account_id = this_campaign.get('account_id')
    return account_id

def search_target_keyword(keyword):
    from facebook_business.adobjects.targetingsearch import TargetingSearch
    params = {
        'q': str(keyword),
        'type': TargetingSearch.TargetingSearchTypes.interest,
    }
    search_target_result_list = TargetingSearch.search(params=params)
    return search_target_result_list


def retrieve_adset_interest_list(origin_adset_id):
    this_adset = facebook_business_adset.AdSet(fbid = origin_adset_id)
    this_adset_interest = this_adset.remote_read(fields=[facebook_business_adset.AdSet.Field.targeting])
    if this_adset_interest.get("targeting") and this_adset_interest.get("targeting").get("flexible_spec"):
        flexible_spec_list = this_adset_interest.get("targeting").get("flexible_spec")
        if len(flexible_spec_list) > 0:
            interests_list = flexible_spec_list[0].get('interests')
            return interests_list

    return None

def get_existed_all_interests(campaign_id):
    camp = facebook_business_campaign.Campaign(campaign_id)
    adset_ids = camp.get_ad_sets(fields = [ facebook_business_adset.AdSet.Field.id ])
    
    # get the existed adset's interest
    campaign_interest_id_list = []
    interest_id_name_mapping = {}
    
    for adset_id in adset_ids:
        adset_id = adset_id.get('id')
        adset_interests = retrieve_adset_interest_list(adset_id)
        if adset_interests:
            for adset_interest in adset_interests:
                adset_interest_id = int(adset_interest.get('id'))
                adset_interest_name = adset_interest.get('name')
                if adset_interest_id and adset_interest_name:
                    if adset_interest_id not in campaign_interest_id_list:
                        interest_id_name_mapping[adset_interest_id] = adset_interest_name
                        campaign_interest_id_list.append(adset_interest_id)
    return campaign_interest_id_list, interest_id_name_mapping 

def get_suggest_interets_by_keyword(interest_id, interest_name):
    
    suggest_id_list = []
    suggest_id_name_mapping = {}
    search_target_result_list = search_target_keyword(interest_name)
#     print('search_target_result_list',search_target_result_list)

    for search_target_result in search_target_result_list:
        if search_target_result.get('id') not in suggest_id_list:
            suggest_id_list.append(search_target_result.get('id'))
            suggest_id_name_mapping[search_target_result.get('id')] = search_target_result.get('name')
#     print('[get_suggest_interets_by_keyword] suggest_id_list:' , suggest_id_list)
#     print('[get_suggest_interets_by_keyword] suggest_id_name_mapping:' , suggest_id_name_mapping)
    return suggest_id_list, suggest_id_name_mapping
    
def save_suggestion_by_adset(account_id, campaign_id, adset_id, suggest_id_set, suggest_id_name_mapping):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    for suggest_id in suggest_id_set:
        suggest_name = suggest_id_name_mapping.get(suggest_id)
#         print('[save_suggestion_by_existed_keyword]', account_id, campaign_id, adset_id, suggest_id , suggest_name)
        sql = "INSERT IGNORE INTO campaign_target_suggestion ( account_id, campaign_id, source_adset_id, suggest_id, suggest_name ) VALUES ( %s, %s, %s, %s, %s )"
        val = ( int(account_id), int(campaign_id), int(adset_id), int(suggest_id), suggest_name)
        my_cursor.execute(sql, val)
        my_db.commit()    
    my_cursor.close()
    my_db.close()

def get_queryed_adset_list(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = 'SELECT distinct source_adset_id FROM campaign_target_suggestion where campaign_id = {}'.format(campaign_id)
    my_cursor.execute(sql)
    result = my_cursor.fetchall()
    my_db.commit()
    my_cursor.close()
    my_db.close()

    query_adset_list = []
    if len(result) > 0:
        for row in result:
            query_adset_list.append(row[0])
    return query_adset_list
    
def get_saved_suggestion_interests(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = 'SELECT suggest_id, suggest_name FROM campaign_target_suggestion where campaign_id = {}'.format(campaign_id)
    my_cursor.execute(sql)
    results = my_cursor.fetchall()
    my_db.commit()
    my_cursor.close()
    my_db.close()
    
    saved_suggest_id_name_dic = {}
    if len(results) > 0:
        for row in results:
            saved_suggest_id_name_dic[row[0]] = row[1]
        
        return saved_suggest_id_name_dic
    else:
        print('[get_saved_suggestion_interests] no saved suggestions')
        return None
    
    
def process_campaign_suggestion(campaign_id):
    print('[process_campaign_suggestion] campaign_id:', campaign_id)

    #get adset which already use to get interest
    queryed_source_adset_list = get_queryed_adset_list(campaign_id)
#     print('[process_campaign_suggestion] queryed_source_adset_list:', queryed_source_adset_list)
    
    #find existed adset as source adset to get suggestion
    account_id = get_account_id_by_campaign(campaign_id)
    camp = facebook_business_campaign.Campaign(campaign_id)
    adset_ids = camp.get_ad_sets(fields = [ facebook_business_adset.AdSet.Field.id ])
    for adset_id in adset_ids:
        adset_id = int(adset_id.get('id'))
        
        # only query suggest interest for new added adset
        if adset_id in queryed_source_adset_list:
            continue
            
        adset_name = get_adset_name(adset_id)  
#         print('[process_campaign_suggestion] adset_id:', adset_id, 'adset_name:', adset_name)

        is_need_ignore = False
        for ignore_str in IGNORE_ADSET_STR_LIST:
            if ignore_str in adset_name:
                is_need_ignore = True
                break
                
        if not is_need_ignore:
            print('[process_campaign_suggestion] adset_id:', adset_id, 'adset_name:', adset_name)

            # get interests in this adset
            this_adset_interest_id_name_list = retrieve_adset_interest_list(adset_id)
            print('[process_campaign_suggestion] this_adset_interest_id_name_list:', this_adset_interest_id_name_list)

            # lookalike adset don't have interest
            if not this_adset_interest_id_name_list:
                continue
                
            # use each interest to find suggest interests
            for this_adset_interest_id_name in this_adset_interest_id_name_list:
                interest_id = this_adset_interest_id_name.get('id')
                interest_name = this_adset_interest_id_name.get('name')
                suggest_id_list, suggest_id_name_mapping = get_suggest_interets_by_keyword(interest_id, interest_name)
                
                # save suggest interests for this adset into database
                this_suggest_set = set(suggest_id_list)
                print('[process_campaign_suggestion] this_suggest_set:', this_suggest_set)
                save_suggestion_by_adset(account_id, campaign_id, adset_id, this_suggest_set, suggest_id_name_mapping)
                
                
            print('===========')
            
    print('--')


def save_suggestion_for_all_campaign():
    campaign_list =  mysql_saver.get_campaign_target().campaign_id.unique().tolist()
    print('[save_suggestion_for_all_campaign] current running campaign:', len(campaign_list), campaign_list )
    
    for campaign_id in campaign_list:
        print('[save_suggestion_for_all_campaign] campaign_id:', campaign_id)
        process_campaign_suggestion(campaign_id)

def get_suggestion_not_used(campaign_id):
    # need to process each time because it may has new added adset
    process_campaign_suggestion(campaign_id)
    
    saved_suggest_id_name_dic = get_saved_suggestion_interests(campaign_id)
    if not saved_suggest_id_name_dic:
        print('[get_suggestion_not_used] saved_suggest_id_name_dic None')
        return
    
    print('[get_suggestion_not_used] saved_suggest_id_name_dic total len:', len(saved_suggest_id_name_dic))
    print(saved_suggest_id_name_dic)
    print('--')
    #need to minus used interest
    campaign_interest_id_list, interest_id_name_mapping = get_existed_all_interests(campaign_id)
    print(campaign_interest_id_list)
    for interest_id in campaign_interest_id_list:
        if interest_id in saved_suggest_id_name_dic:
            del saved_suggest_id_name_dic[interest_id]
    
    print('[get_suggestion_not_used] saved_suggest_id_name_dic not use, len:', len(saved_suggest_id_name_dic))
    return saved_suggest_id_name_dic
    
def main():
    # only save suggestion once for each campaign
#     save_suggestion_for_all_campaign()
    get_suggestion_not_used(23843467729120098)
    
#     process_campaign_suggestion(23843467729120098)
    
        
if __name__ == "__main__":
    main()


# In[ ]:




