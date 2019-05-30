#!/usr/bin/env python
# coding: utf-8

# In[26]:


from pathlib import Path
import datetime
import time

from facebook_business.api import FacebookAdsApi
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

def get_existed_adset_interests(campaign_id):
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

def get_suggest_interets(campaign_interest_id_list, interest_id_name_mapping):
    
    #use existed adset interest to find suggest interest
    #get suggest list for each keywork
    suggest_id_list = []
    suggest_id_name_mapping = {}
    for campaign_interest_id in campaign_interest_id_list:
        campaign_interest_name = interest_id_name_mapping.get(campaign_interest_id)
#         print('campaign_interest_id:', campaign_interest_id , 'campaign_interest_id:',campaign_interest_name)
        
        search_target_result_list = search_target_keyword(campaign_interest_name)
#         print('search_target_result_list',search_target_result_list)

        for search_target_result in search_target_result_list:
            if search_target_result.get('id') not in suggest_id_list:
                suggest_id_list.append(search_target_result.get('id'))
                suggest_id_name_mapping[search_target_result.get('id')] = search_target_result.get('name')
    print('suggest_id_list:' , suggest_id_list)
    print('suggest_id_name_mapping:' , suggest_id_name_mapping)
    return suggest_id_list, suggest_id_name_mapping
    
def save_suggestion_by_existed_keyword(campaign_id):
    account_id = get_account_id_by_campaign(campaign_id)
    campaign_interest_id_list, interest_id_name_mapping = get_existed_adset_interests(campaign_id)
    if len(campaign_interest_id_list) == 0:
        return
#     print('[save_suggestion_by_existed_keyword] campaign_interest_id_list:' , campaign_interest_id_list)
#     print('[save_suggestion_by_existed_keyword] interest_id_name_mapping:' , interest_id_name_mapping)
    
    suggest_id_list, suggest_id_name_mapping = get_suggest_interets(campaign_interest_id_list, interest_id_name_mapping)
    if len(suggest_id_list) == 0:
        return
#     print('[save_suggestion_by_existed_keyword] suggest_id_list:' , suggest_id_list)
#     print('[save_suggestion_by_existed_keyword] suggest_id_name_mapping:' , suggest_id_name_mapping)
    
    if len(suggest_id_list) == 0:
        return
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    for suggest_id in suggest_id_list:
        suggest_name = suggest_id_name_mapping.get(suggest_id)
        print('[save_suggestion_by_existed_keyword]', account_id, campaign_id, suggest_id , suggest_name)
        sql = "INSERT IGNORE INTO campaign_target_suggestion ( account_id, campaign_id, suggest_id, suggest_name ) VALUES ( %s, %s, %s, %s )"
        val = ( account_id, campaign_id, suggest_id, suggest_name )
        my_cursor.execute(sql, val)
        my_db.commit()
    my_db.close()

def is_suggested(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = 'SELECT * FROM campaign_target_suggestion where campaign_id = {}'.format(campaign_id)
    my_cursor.execute(sql)
    result = my_cursor.fetchall()
    my_db.commit()
    my_db.close()
    return len(result) > 0
    
def get_saved_suggestion_interests(campaign_id):
    my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
    my_cursor = my_db.cursor()
    sql = 'SELECT suggest_id, suggest_name FROM campaign_target_suggestion where campaign_id = {}'.format(campaign_id)
    my_cursor.execute(sql)
    results = my_cursor.fetchall()
    my_db.commit()
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
    if is_suggested(campaign_id):
        print('[process_campaign_suggestion] is suggested')
    else:
        print('[process_campaign_suggestion] not suggested')
        save_suggestion_by_existed_keyword(campaign_id)

def save_suggestion_for_all_campaign():
    campaign_list =  mysql_saver.get_campaign_target().campaign_id.unique().tolist()
    print('[save_suggestion_for_all_campaign] current running campaign:', len(campaign_list), campaign_list )
    
    for campaign_id in campaign_list:
        print('[save_suggestion_for_all_campaign] campaign_id:', campaign_id)
        process_campaign_suggestion(campaign_id)

def get_suggestion_not_used(campaign_id):
    # check has already process this campaign first
    process_campaign_suggestion(campaign_id)
    
    saved_suggest_id_name_dic = get_saved_suggestion_interests(campaign_id)
    if not saved_suggest_id_name_dic:
        print('[get_suggestion_not_used] saved_suggest_id_name_dic None')
        return
    
    print('[get_suggestion_not_used] saved_suggest_id_name_dic len:', len(saved_suggest_id_name_dic))
    print(saved_suggest_id_name_dic)
    print('--')
    #need to minus used interest
    campaign_interest_id_list, interest_id_name_mapping = get_existed_adset_interests(campaign_id)
    print(campaign_interest_id_list)
    for interest_id in campaign_interest_id_list:
        if interest_id in saved_suggest_id_name_dic:
            del saved_suggest_id_name_dic[interest_id]
    
    print('[get_suggestion_not_used] saved_suggest_id_name_dic len:', len(saved_suggest_id_name_dic))
    return saved_suggest_id_name_dic
    
def main():
    # only save suggestion once for each campaign
    save_suggestion_for_all_campaign()
#     get_suggestion_not_used(23843467729120098)
    
        
if __name__ == "__main__":
    main()


# In[ ]:




