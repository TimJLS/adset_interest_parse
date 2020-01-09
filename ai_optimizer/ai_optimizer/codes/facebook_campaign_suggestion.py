#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pathlib import Path
import datetime
import time
import facebook_business.adobjects.adset as facebook_business_adset
import facebook_business.adobjects.campaign as facebook_business_campaign
import facebook_business.adobjects.adsinsights as facebook_business_adsinsights

import facebook_datacollector as fb_collector
import database_controller

import adgeek_permission as permission

IGNORE_ADSET_STR_LIST = ['AI', 'Copy', 'COPY', 'Lookalike', 'RT', 'Look-a-like']


# In[ ]:


def get_adset_name(adset_id):
    this_adset = facebook_business_adset.AdSet( adset_id).api_get(fields=[ "name" ])
    return this_adset.get('name')


# In[ ]:


def get_account_id_by_campaign(campaign_id):
    this_campaign = facebook_business_campaign.Campaign( campaign_id ).api_get(fields=["account_id"])
    account_id = this_campaign.get('account_id')
    return account_id


# In[ ]:


def search_target_keyword(keyword):
    from facebook_business.adobjects.targetingsearch import TargetingSearch
    params = {
        'q': str(keyword),
        'type': TargetingSearch.TargetingSearchTypes.interest,
    }
    search_target_result_list = TargetingSearch.search(params=params)
    return search_target_result_list


# In[ ]:


def retrieve_adset_interest_list(origin_adset_id):
    this_adset = facebook_business_adset.AdSet(fbid = origin_adset_id)
    this_adset_interest = this_adset.api_get(fields=[facebook_business_adset.AdSet.Field.targeting])
    if this_adset_interest.get("targeting") and this_adset_interest.get("targeting").get("flexible_spec"):
        flexible_spec_list = this_adset_interest.get("targeting").get("flexible_spec")
        if len(flexible_spec_list) > 0:
            interests_list = flexible_spec_list[0].get('interests')
            return interests_list

    return None


# In[ ]:


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


# In[ ]:


def get_suggest_interets_by_keyword(interest_id, interest_name):
    
    suggest_id_list = []
    suggest_id_name_mapping = {}
    suggest_id_size_mapping = {}
    search_target_result_list = search_target_keyword(interest_name)
    print('[get_suggest_interets_by_keyword] search_target_result_list',search_target_result_list)

    for search_target_result in search_target_result_list:
        if search_target_result.get('id') not in suggest_id_list:
            suggest_id_list.append(search_target_result.get('id'))
            suggest_id_name_mapping[search_target_result.get('id')] = search_target_result.get('name')
            suggest_id_size_mapping[search_target_result.get('id')] = search_target_result.get('audience_size')
#     print('[get_suggest_interets_by_keyword] suggest_id_list:' , suggest_id_list)
    print('[get_suggest_interets_by_keyword] suggest_id_name_mapping:' , suggest_id_name_mapping)
    return suggest_id_list, suggest_id_name_mapping, suggest_id_size_mapping


# In[ ]:


def save_suggestion_by_adset(account_id, campaign_id, adset_id, suggest_id_set, suggest_id_name_mapping, suggest_id_size_mapping):
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    for suggest_id in suggest_id_set:
        suggest_name = suggest_id_name_mapping.get(suggest_id)
        audience_size = suggest_id_size_mapping.get(suggest_id)
        
        database_fb.insert_ignore(
            "campaign_target_suggestion",
            {
                'account_id': int(account_id),
                'campaign_id': int(campaign_id),
                'source_adset_id': int(adset_id),
                'suggest_id': int(suggest_id),
                'suggest_name': suggest_name,
                'audience_size': int(audience_size),
            }
        )


# In[ ]:


def get_queryed_adset_list(campaign_id):
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    df_source_adset_id = database_fb.retrieve("campaign_target_suggestion", campaign_id, by_request_time=False)
    if not df_source_adset_id.empty:
        return df_source_adset_id['source_adset_id'].unique().tolist()
    else:
        return []


# In[ ]:


def get_saved_suggestion_interests(campaign_id):
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    df = database_fb.retrieve("campaign_target_suggestion", campaign_id, by_request_time=False)
    df = df[['suggest_id', 'suggest_name', 'audience_size']]
    
    saved_suggest_id_name_dic = {}
    saved_suggest_id_size_dic = {}
    if not df.empty:
        for (idx, row,) in df.iterrows():
            row['suggest_id'], row['suggest_name'], row['audience_size']
            saved_suggest_id_name_dic[row['suggest_id']] = row['suggest_name']
            saved_suggest_id_size_dic[row['suggest_id']] = int(row['audience_size'])
    else:
        print('[get_saved_suggestion_interests] no saved suggestions')
    return saved_suggest_id_name_dic, saved_suggest_id_size_dic


# In[ ]:


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
                suggest_id_list, suggest_id_name_mapping, suggest_id_size_mapping = get_suggest_interets_by_keyword(interest_id, interest_name)
                
                # save suggest interests for this adset into database
                this_suggest_set = set(suggest_id_list)
                print('[process_campaign_suggestion] this_suggest_set:', this_suggest_set)
                save_suggestion_by_adset(account_id, campaign_id, adset_id, this_suggest_set, suggest_id_name_mapping, suggest_id_size_mapping)
                
                
            print('===========')
            
    print('--')


# In[ ]:


def save_suggestion_for_all_campaign():
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    campaign_list = database_fb.get_running_campaign().to_dict('records')
    print('[save_suggestion_for_all_campaign] current running campaign:', len(campaign_list), campaign_list )
    
    for campaign in campaign_list:
        print('[save_suggestion_for_all_campaign] campaign_id:', campaign.get("campaign_id"))
        process_campaign_suggestion(campaign.get("campaign_id"))


# In[ ]:


def get_suggestion_not_used(campaign_id):
    # need to process each time because it may has new added adset
    process_campaign_suggestion(campaign_id)
    
    saved_suggest_id_name_dic, saved_suggest_id_size_dic = get_saved_suggestion_interests(campaign_id)
    if not saved_suggest_id_name_dic:
        print('[get_suggestion_not_used] saved_suggest_id_name_dic None')
        return None, None
    
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
    return saved_suggest_id_name_dic, saved_suggest_id_size_dic


# In[ ]:


def main():
    # only save suggestion once for each campaign
    import adgeek_permission as permission
    a_id = 237880526824540
    c_id = 23844100165680431
    permission.init_facebook_api(a_id)
    saved_suggest_id_name_dic, saved_suggest_id_size_dic  = get_suggestion_not_used(c_id)
    print('[make_suggest_adset] saved_suggest_id_name_dic', saved_suggest_id_name_dic, 'saved_suggest_id_size_dic', saved_suggest_id_size_dic)


# In[ ]:


if __name__ == "__main__":
    main()


# In[ ]:


# !jupyter nbconvert --to script facebook_campaign_suggestion.ipynb


# In[ ]:




