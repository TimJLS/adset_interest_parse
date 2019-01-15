from facebook_datacollector import Campaigns
from facebook_adapter import FacebookCampaignAdapter
from pathlib import Path
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.targeting import Targeting
from facebook_business.api import FacebookRequest
import copy
import os
import mysql_adactivity_save
import pandas as pd
import datetime

my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

campaign_objective = {
    'LINK_CLICKS': 'link_click',
    'POST_ENGAGEMENT': 'post_engagement', 
    'VIDEO_VIEWS': 'video_view', 
    'CONVERSIONS':'offsite_conversion',
}
DATABASE = 'ad_activity'
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

def copy_adset(adset_id):
    request = FacebookRequest(
            node_id=adset_id,
            method='POST',
            endpoint='/copies',
            api_type='EDGE',
    )
    params = {
            'deep_copy': True,
    }
    request.add_params(params)    
    response = request.execute()
    response_json = response.json()
#     print(response_json)
    new_adset_id = response_json.get('copied_adset_id')
#     print(new_adset_id)
    return new_adset_id


def modify_exists_adset(adset_id, adset_params):
    adset = AdSet(adset_id)
    
    for key in adset_params:
#         print(key, adset_params[key])
        adset[key] = adset_params[key]
    #print('[modify_exists_adset] adset:' , str(adset))
    update_response = adset.update()
    #print('update_response:' , update_response)
    remote_update_response = adset.remote_update()
    #print('remote_update_response:' , remote_update_response)
    
def config_adset_params_by_age(new_adset_id, age_max, age_min, init_bid=None):
    
    fields_adSet = [  AdSet.Field.campaign_id,  AdSet.Field.name, AdSet.Field.bid_amount, AdSet.Field.bid_strategy, AdSet.Field.daily_budget, AdSet.Field.budget_remaining, AdSet.Field.optimization_goal, AdSet.Field.bid_info, AdSet.Field.pacing_type
                    , AdSet.Field.attribution_spec, AdSet.Field.targeting]
    ad_set = AdSet(new_adset_id)
    ad_set_data = ad_set.remote_read(fields = fields_adSet)
    
    target = ad_set_data[AdSet.Field.targeting]
    original_name = ad_set_data[AdSet.Field.name]
                     
    target['age_max'] =  age_max
    target['age_min'] =  age_min

    if init_bid is None:
        adset_params = {
            AdSet.Field.name: original_name + ' ' +str(age_min) + '-' + str(age_max) ,
            AdSet.Field.targeting: target,
            'status': AdSet.Status.active,
        }
        ad_groups = ad_set.get_ads(fields=[
            AdSet.Field.name,
            AdSet.Field.campaign_id,
            AdSet.Field.configured_status,
        ])
    else:
        adset_params = {
            AdSet.Field.name: original_name + ' ' +str(age_min) + '-' + str(age_max) ,
            AdSet.Field.targeting: target,
            AdSet.Field.bid_amount: init_bid+1,
            'status': AdSet.Status.active,
        }
        ad_groups = ad_set.get_ads(fields=[
            AdSet.Field.name,
            AdSet.Field.campaign_id,
            AdSet.Field.bid_amount,
            AdSet.Field.configured_status,
        ])
    for ad in ad_groups:
        ad_id = ad['id']
#         print('ad_id' , ad_id)
        ad_object = Ad(ad_id)
        ad_object['status'] = Ad.Status.active
        ad_object.remote_update()
    return adset_params

def duplicate_asset_by_more_target(adset_id_which_want_copy, init_bid=None, bid_adjust=False):
    # duplicate more age target , use original age interval
    
    ad_set = AdSet(adset_id_which_want_copy)
    ad_set_data = ad_set.remote_read(fields = [AdSet.Field.targeting])
    target = ad_set_data[AdSet.Field.targeting]
    age_max = target['age_max']
    age_min = target['age_min']    
#     print('original age interval: ', age_min, '-' , age_max)
    
    interval_count = 2
    age_interval = int((age_max - age_min)/interval_count)
    
    current_adset_min = age_min
    current_adset_max = current_adset_min + age_interval
  
    for i in range(interval_count):
#         print(current_adset_min, current_adset_max)
        new_adset_id = copy_adset(adset_id_which_want_copy)
#         adset_params = config_adset_params_by_age(new_adset_id, current_adset_max, current_adset_min, init_bid)
        if bid_adjust is True and init_bid is not None:
            adset_params = config_adset_params_by_age(new_adset_id, current_adset_max, current_adset_min, init_bid)
        else:
            adset_params = config_adset_params_by_age(new_adset_id, current_adset_max, current_adset_min)
        modify_exists_adset(new_adset_id, adset_params)
        
        current_adset_min += age_interval
        current_adset_max += age_interval
        
def check_if_campaign_day_target_reach(campaign):
    mydict = Campaigns(campaign).get_campaign_insights()
    
    fb = FacebookCampaignAdapter(campaign)
    fb.get_df()
    fb.get_bid()
    fb.get_campaign_days_left()
    
    charge_type = campaign_objective[Campaigns( campaign ).get_campaign_feature().get("target_type")]
    target_dict=dict()
    summary_dict=dict()
    for dic in mydict[0].get("actions"):
        if dic.get("action_type") == charge_type:
            summary_dict[campaign] = int( dic.get("value") )
            
    target_left = mysql_adactivity_save.get_campaign_target_left_dict()[campaign].iloc[0]
    campaign_days_left = fb.campaign_days_left
    charge_per_day = target_left/campaign_days_left
    target_dict[campaign] = charge_per_day
    achieving_rate = summary_dict[campaign] / target_dict[campaign]
    print(achieving_rate)
    if achieving_rate > 0.8 and achieving_rate < 1:
        duplicate_highest_ranking_adset(campaign)
    elif achieving_rate < 0.8:
        duplicate_highest_ranking_adset_with_higher_bid(campaign, fb)
    return
#     try:
#         target_left = mysql_adactivity_save.get_campaign_target_left_dict()[campaign].iloc[0]
#         campaign_days = Campaigns(campaign).get_campaign_feature()['campaign_days']
#         charge_per_day = target_left/campaign_days
#         target_dict[campaign] = charge_per_day
#         achieving_rate = summary_dict[campaign] / target_dict[campaign]
#         print(achieving_rate)
#         if achieving_rate > 0.8 and achieving_rate < 1:
#             duplicate_highest_ranking_adset(campaign)
#         elif achieving_rate < 0.8:
#             duplicate_highest_ranking_adset_with_higher_bid(campaign)
#         return
#     except:
#         pass
    
def duplicate_highest_ranking_adset(campaign):
    mydb=mysql_adactivity_save.connectDB(DATABASE)
    df = pd.read_sql("select * from adset_score where campaign_id=%s" %(campaign), con=mydb)
    df = df[df.request_time.dt.date==yesterday].sort_values(by=['score'], ascending=False)
    adset_id = df['adset_id'].iloc[0]
    duplicate_asset_by_more_target(adset_id, bid_adjust=False)
    return

def duplicate_highest_ranking_adset_with_higher_bid(campaign, fb):
    yesterday = datetime.datetime.now().date() - datetime.timedelta(2)
    mydb=mysql_adactivity_save.connectDB(DATABASE)
    df = pd.read_sql("select * from adset_score where campaign_id=%s" %(campaign), con=mydb)
    df = df[df.request_time.dt.date==yesterday].sort_values(by=['score'], ascending=False)
#     print(df)
#     return
    adset_id = df['adset_id'].iloc[0]
    init_bid = fb.init_bid_dict[ df['adset_id'].iloc[0] ]
    duplicate_asset_by_more_target(adset_id, init_bid, bid_adjust=True)
    return
            
if __name__ == "__main__":
    campaign_list = mysql_adactivity_save.get_campaign()
    for campaign in campaign_list:
        check_if_campaign_day_target_reach(campaign)
