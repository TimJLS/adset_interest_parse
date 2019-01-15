import numpy as np
import pandas as pd
from pathlib import Path
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adcreative import AdCreative
import os
import datetime

import mysql_adactivity_save
import optimizer
import fb_graph

my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

COL_CAMPAIGN_ID, COL_ADSET_ID = 'campaign_id', 'adset_id'
HOUR_PER_DAY = 18
CENTER = 0.5
WIDTH = 10
BID_RANGE = 0.5
def normalized_sigmoid_fkt(center, width, x):

    s= 1 / (1 + np.exp( width * ( x - center ) ) )
    return s

def revive_bidding(ad_id):
    ad = Ad(ad_id)
    ad_insights = ad.get_insights(fields = [COL_CAMPAIGN_ID, COL_ADSET_ID])
    for ad_insight in ad_insights:
        campaign_id, adset_id = ad_insight.get(COL_CAMPAIGN_ID), ad_insight.get(COL_ADSET_ID)
    
    TOTAL_CLICKS = mysql_adactivity_save.get_total_clicks(campaign_id)
    
    avgspeed, speed, _ = mysql_adactivity_save.get_speed(ad_id)
    speed = optimizer.compute_speed(ad_id)
    init_cpc = mysql_adactivity_save.get_init_bidamount(ad_id)
    progress = speed / avgspeed
    if progress > 1:
        next_cpc = init_cpc
    else:
        next_cpc = init_cpc + BID_RANGE * init_cpc*( normalized_sigmoid_fkt(CENTER, WIDTH, progress) - 0.5 )

    return next_cpc

def bid_adjust(campaign_id):
    ad_id_list = mysql_adactivity_save.get_ad_id( campaign_id )
    campaignid_dict = mysql_adactivity_save.get_campaignid_target()
    total_clicks = campaignid_dict[ int(campaign_id) ]
    for ad_id in ad_id_list:
        avgspeed, speed, decide_type = mysql_adactivity_save.get_speed(ad_id)
        ad_id = ad_id.astype(dtype=object)
#         print(avgspeed, speed, decide_type)
        bid = revive_bidding(ad_id)
        print(bid, type(bid))
#         bid = bid.astype(dtype=object)
#         mysql_adactivity_save.update_bidcap(ad_id, bid)
        print(ad_id, bid, avgspeed, speed)
    return

def get_target_speed(ad_campaign_id, TOTAL_CLICKS):
    ad_campaign = Campaign(ad_campaign_id)
    adsets = ad_campaign.get_ad_sets( fields=[ AdSet.Field.id ] )
    spend_cap, objective, start_time, stop_time = fb_graph.getFeatureByCampaign(ad_campaign_id)
    start_time = datetime.datetime.strptime( start_time, '%Y-%m-%dT%H:%M:%S+%f' )
    stop_time = datetime.datetime.strptime( stop_time, '%Y-%m-%dT%H:%M:%S+%f' )
    campaign_days = ( stop_time - start_time ).days
    avgspeed = int(TOTAL_CLICKS) / campaign_days / HOUR_PER_DAY
    return avgspeed

if __name__ == "__main__":
    FacebookAdsApi.init( my_app_id, my_app_secret, my_access_token )
#     revive_bidding(23843052139460246)
    bid_adjust(23843052139470246)