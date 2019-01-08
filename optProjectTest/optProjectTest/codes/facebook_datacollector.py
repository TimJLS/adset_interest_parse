from pathlib import Path
import sys
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adactivity import AdActivity
from facebook_business.adobjects.insightsresult import InsightsResult
from facebook_business.adobjects.adsinsights import AdsInsights
my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
import numpy as np
import math
import json
import datetime
import pandas as pd
import mysql_adactivity_save
LIMIT=10000
HOUR_PER_DAY = 20
BID_RANGE = 0.8
PRED_CPC, PRED_BUDGET, REASONS, DECIDE_TYPE, STATUS, ADSET = 'pred_cpc', 'pred_budget', 'reasons', 'decide_type', 'status', 'adset_id'
campaign_objective = {
    'LINK_CLICKS': 'link_click',
    'POST_ENGAGEMENT': 'post_engagement', 
    'VIDEO_VIEWS': 'video_view', 
    'CONVERSIONS':'offsite_conversion',
}
target_index = {'LINK_CLICKS': 'link_click', 'POST_ENGAGEMENT': 'post_engagement', 'VIDEO_VIEWS': 'video_view' }
charge_index = {'LINK_CLICKS': 'link_click', 'POST_ENGAGEMENT': 'post_engagement', 'VIDEO_VIEWS': 'video_view', 'CLICKS': 'clicks' }
target_cpc = {'CLICKS':AdsInsights.Field.cpc}
target_charge={'CLICKS':AdsInsights.Field.clicks}
fields_ad = [ AdsInsights.Field.ad_id, AdsInsights.Field.impressions, AdsInsights.Field.reach ]
insights_ad = [ AdsInsights.Field.actions, AdsInsights.Field.cost_per_action_type ]
field_clicks = ['cpc', 'clicks']
HOUR_TIME = 'hourly_stats_aggregated_by_audience_time_zone'
AGE = 'age'
GENDER = 'gender'
# breakdowns = [AGE, GENDER]
breakdowns = [HOUR_TIME]
fields_adSet = [ AdSet.Field.id, AdSet.Field.bid_amount, AdSet.Field.daily_budget, AdSet.Field.budget_remaining, AdSet.Field.optimization_goal, AdSet.Field.targeting, AdSet.Field.effective_status ]
insights_adSet = [AdsInsights.Field.spend]
COL_CAMPAIGN_ID, COL_ADSET_ID = 'campaign_id', 'adset_id'
COL_TARGET_TYPE = 'target_type'
COL_TARGET_CPC = 'target_cpc'
COL_CHARGE_CPC = 'charge_cpc'
COL_TARGET = 'target'
COL_CHARGE = 'charge'
COL_OPTIMAL_GOAL, COL_BID_AMOUNT, COL_DAILY_BUDGET, COL_AGE_MAX, COL_AGE_MIN, COL_SPEND = 'optimization_goal', 'bid_amount', 'daily_budget', 'age_max', 'age_min', 'spend'
COL_FLEXIBLE_SPEC, COL_GEO_LOCATIONS = 'flexible_spec', 'geo_locations'
COL_CAMPAIGN_ID, COL_ADSET_ID = 'campaign_id', 'adset_id'
insights_camp=[AdsInsights.Field.spend, AdsInsights.Field.actions, AdsInsights.Field.impressions]
fields_camp=[ Campaign.Field.id, Campaign.Field.spend_cap, Campaign.Field.start_time, Campaign.Field.stop_time, Campaign.Field.objective]
COL_SPEND_CAP, COL_OBJECTIVE, COL_START_TIME, COL_STOP_TIME = 'spend_cap', 'objective', 'start_time', 'stop_time'
FEATURE_CAMPAIGN = [ 'id','spend_cap', 'objective', 'start_time', 'stop_time' ]
COL_CAMPAIGN = [ 'campaign_id','spend_cap', 'objective', 'start_time', 'stop_time' ]
FEATURE_ADSET = [COL_ADSET_ID, COL_OPTIMAL_GOAL, COL_BID_AMOUNT,COL_DAILY_BUDGET,
                 COL_AGE_MAX,COL_AGE_MIN,COL_FLEXIBLE_SPEC,COL_GEO_LOCATIONS,COL_SPEND,'request_time']
class Accounts(object):
    def __init__( self, account_id ):
        self.account_id = account_id
    def get_account_insights( self ):
        accounts = AdAccount( self.account_id )
        params = {
            'date_preset': 'today',
        }
        account = accounts.get_insights(params=params,
                                        fields=[AdsInsights.Field.account_id,
                                               AdsInsights.Field.cpc,
                                               AdsInsights.Field.clicks,
                                               AdsInsights.Field.spend,
                                               AdsInsights.Field.impressions]
                                       )
        return account
class Campaigns(object):
    def __init__( self, campaign_id ):
        self.campaign_id = campaign_id
    def get_campaign_feature( self ):
        campaign_feature_dict=dict(  )
        ad_campaign = Campaign( self.campaign_id )
        adcamps = ad_campaign.remote_read( fields=fields_camp )
        for campaign_feature in FEATURE_CAMPAIGN:
            campaign_feature_dict[ campaign_feature ]=adcamps.get( campaign_feature )
        campaign_feature_dict[COL_CAMPAIGN_ID] = campaign_feature_dict.pop('id')
        campaign_feature_dict[COL_TARGET_TYPE] = campaign_feature_dict.pop('objective')
#         df = pd.DataFrame(campaign_feature_dict, index=[0])
        campaign_feature_dict['start_time'] = datetime.datetime.strptime( campaign_feature_dict['start_time'],'%Y-%m-%dT%H:%M:%S+%f' )
        campaign_feature_dict['stop_time'] = datetime.datetime.strptime( campaign_feature_dict['stop_time'],'%Y-%m-%dT%H:%M:%S+%f' )
        campaign_feature_dict['campaign_days'] = ( campaign_feature_dict['stop_time'] - campaign_feature_dict['start_time'] ).days
        campaign_feature_dict['start_time'] = campaign_feature_dict['start_time'].strftime( '%Y-%m-%d %H:%M:%S' )
        campaign_feature_dict['stop_time'] = campaign_feature_dict['stop_time'].strftime( '%Y-%m-%d %H:%M:%S' )
        campaign_feature_dict['budget_per_day'] = int(campaign_feature_dict['spend_cap'])/campaign_feature_dict['campaign_days']
#         mysql_adactivity_save.intoDB("campaign_target", df)
        return campaign_feature_dict
    def get_campaign_insights( self ):
        campaign = Campaign( self.campaign_id )
        params = {
            'date_preset': 'today',
        }
        insights_cpc = []
        df = mysql_adactivity_save.get_campaign_target( self.campaign_id )
        if df['charge_type'].iloc[0] == 'CLICKS':
            insights_cpc.append( target_cpc[ df['charge_type'].iloc[0] ] )
            insights_cpc.append( target_charge[ df['charge_type'].iloc[0] ] )
        else:
            insights_cpc.append( AdsInsights.Field.cost_per_action_type )
        insights = campaign.get_insights(params=params, fields=insights_camp+insights_cpc)
        
        return insights
    def get_lifetime_insights(self):
        campaign = Campaign( self.campaign_id )
        params = {
            'date_preset': 'lifetime',
        }
        insights_cpc = []
        df = mysql_adactivity_save.get_campaign_target( self.campaign_id )
        if df['charge_type'].iloc[0] == 'CLICKS':
            insights_cpc.append( target_cpc[ df['charge_type'].iloc[0] ] )
            insights_cpc.append( target_charge[ df['charge_type'].iloc[0] ] )
        else:
            insights_cpc.append( AdsInsights.Field.cost_per_action_type )
        insights = campaign.get_insights(params=params, fields=insights_camp+insights_cpc)
        return insights
    def get_adids( self ):
        ad_id_list=list()
        ad_campaign = Campaign( self.campaign_id )
        ads = ad_campaign.get_ads( fields = [ Ad.Field.id ])
        for ad in ads:
            ad_id_list.append( ad.get("id") )
        return ad_id_list
    def get_account_id( self ):
        campaign = Campaign( self.campaign_id )
        account = campaign.get_insights(fields=[Campaign.Field.account_id])
        for acc in account:
            acc_id = acc.get("account_id")
            return acc_id

class AdSets(object):
    def __init__( self, adset_id ):
        self.adset_id = adset_id
    def get_adset_insights( self ):
        ad_set = AdSet( self.adset_id )
        params = {
            "fields" : ",".join(insights_adSet),
            'limit' : 10000,
            'date_preset':'today',
        }
        adsets = ad_set.get_insights(params=params)
        for adset in adsets:
            spend = int(adset.get(COL_SPEND))
            adsets = ad_set.remote_read( fields = fields_adSet, params = params )
            optimal_goal = adsets.get( COL_OPTIMAL_GOAL )
            bid_amount, daily_budget = adsets.get( COL_BID_AMOUNT ), adsets.get( COL_DAILY_BUDGET )
            age_max = adsets.get( AdSet.Field.targeting ).get( COL_AGE_MAX )
            age_min = adsets.get( AdSet.Field.targeting ).get( COL_AGE_MIN )
            flexible_spec = str( adsets.get( AdSet.Field.targeting ).get( COL_FLEXIBLE_SPEC ) )
            geo_locations = str( adsets.get( AdSet.Field.targeting ).get( COL_GEO_LOCATIONS ) )
            adset_insights_keys=FEATURE_ADSET
            adset_insights_values=[ self.adset_id, optimal_goal, bid_amount, daily_budget,
                                       age_max, age_min, flexible_spec, geo_locations, spend, datetime.datetime.now() ]
            adset_insight_dict = dict(zip(adset_insights_keys, adset_insights_values))
            df = pd.DataFrame(adset_insight_dict, index=[0])
    #         mysql_adactivity_save.intoDB("adset_insights", df)
            return df

class Ads( AdSets, Campaigns ):
    def __init__( self, ad_id ):
        self.ad_id = ad_id
    def get_camp_id( self ):
        ad = Ad( self.ad_id )
        campaign = ad.remote_read( fields=[ Ad.Field.campaign_id ] )
        return int( campaign.get( COL_CAMPAIGN_ID ) )
    def get_adset_id(self):
        ad = Ad( self.ad_id )
        adset = ad.remote_read( fields=[ Ad.Field.adset_id ] )
        return int( adset.get( COL_ADSET_ID ) )    
    def get_ad_insights( self ):
        insight_dict = dict()
        insight = list()
        ad = Ad( self.ad_id )
        params = {
            "fields" : ",".join( fields_ad + field_clicks + insights_ad ),
            "date_preset" : 'today', 
        }
        ad_insights = ad.get_insights( params=params )
        ads = Ads( self.ad_id )
        df = mysql_adactivity_save.get_campaign_target( ads.get_camp_id() )
        target_type = Campaigns( ads.get_camp_id() ).get_campaign_feature()[COL_TARGET_TYPE]
#         charge_type = target_type
        charge_type = df['charge_type'][df.campaign_id==ads.get_camp_id()].iloc[0]

        for field in fields_ad:
            insight_dict[field]=ad_insights[0].get(field)
        charge_type = 'CLICKS'
        for field in insights_ad:
            insight_dict[COL_TARGET] = '0'
            insight_dict[COL_CHARGE] = '0'
            insight_dict[COL_TARGET_CPC] = '0'
            insight_dict[COL_CHARGE_CPC] = '0'
            for act in ad_insights[0].get('actions'):
                if act['action_type']==campaign_objective[target_type]:
                    insight_dict[COL_TARGET] = act['value']
                    insight_dict[COL_CHARGE] = act['value']
            for act in ad_insights[0].get('cost_per_action_type'):
                if act['action_type']==campaign_objective[target_type]:
                    insight_dict[COL_TARGET_CPC] = act['value']
                    insight_dict[COL_CHARGE_CPC] = act['value']

        if charge_type == 'CLICKS':
            insight_dict[COL_CHARGE] = '0'
            insight_dict[COL_CHARGE_CPC] = '0'         
            insight_dict[COL_CHARGE] = ad_insights[0].get('clicks')
            insight_dict[COL_CHARGE_CPC] = ad_insights[0].get('cpc')
               
        return insight_dict
    def get_campaign_feature( self ):
        ad = Ads( self.ad_id )
        campaign = Campaigns( ad.get_camp_id() )
        return campaign.get_campaign_feature()
    def get_adset_insights( self ):
        ad = Ads( self.ad_id )
        adset = AdSets( ad.get_adset_id() )
        return adset.get_adset_insights()

def make_default( campaign_id ):
    ad_list = Campaigns(campaign_id).get_adids()
    mydict=dict()
    for ad in ad_list:
        df_adset = Ads(ad).get_adset_insights()
        adset_id = Ads(ad).get_adset_id()
        mydict[ str(ad) ] = {PRED_CPC: str(df_adset['bid_amount'].iloc[0]),
                             PRED_BUDGET: str(df_adset['daily_budget'].iloc[0]),
                             REASONS: "Requirements not match",
                             DECIDE_TYPE: "Learning",
                             STATUS: True, ADSET: str(adset_id),}
    mydict_json = json.dumps(mydict)
    mysql_adactivity_save.insert_default( str( campaign_id ), mydict_json, datetime.datetime.now() ) 
    return
def check_lifetime_target( campaign_id ):
    insights = Campaigns( campaign_id ).get_lifetime_insights()
    df = mysql_adactivity_save.get_campaign_target( campaign_id )
    charge_type = df['charge_type'].iloc[0]
    field = charge_index[charge_type]
    charge = int( insights[0].get(field) )
#     campaign_target = df['target'].iloc[0] - charge
    campaign_lifetime=dict()
    campaign_lifetime['target']=charge
    return campaign_lifetime

def data_collect( campaign_id, total_clicks, charge_type ):
    request_dict = {'request_time': datetime.datetime.now()}
    charge_dict = {'charge_type': charge_type}
    target_dict = {'target': total_clicks}
    lifetime_target = check_lifetime_target( campaign_id )
    charge = lifetime_target['target']
    target_left_dict = {'target_left': int(total_clicks) - int(charge)}
    campaign_dict = {**Campaigns( campaign_id ).get_campaign_feature(), **charge_dict, **target_left_dict, **target_dict}
    df_camp = pd.DataFrame(campaign_dict, index=[0])
    mysql_adactivity_save.update_campaign_target(df_camp)#update to campaign_target
    for ad in Campaigns( campaign_id ).get_adids():
        if Ads(ad).get_ad_insights()!=None:
            ad_dict = {**Ads(ad).get_ad_insights(),
                       **{'adset_id':Ads(ad).get_adset_id()},
                       **{'campaign_id':Ads(ad).get_camp_id()},
                       **request_dict}
            df_ad = pd.DataFrame(ad_dict, index=[0])
            df_adset = Ads(ad).get_adset_insights()
            mysql_adactivity_save.intoDB("ad_insights", df_ad)#insert into ad_insights
            mysql_adactivity_save.intoDB("adset_insights", df_adset)#insert into adset_insights
    return

def main():
    start_time = datetime.datetime.now()
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    campaignid_target_dict = mysql_adactivity_save.get_campaign_target_dict()
    for campaign_id in campaignid_target_dict:
        charge = campaignid_target_dict.get(campaign_id)
        df = mysql_adactivity_save.get_campaign_target(campaign_id)
        data_collect( campaign_id.astype(dtype=object), charge.iloc[0].astype(dtype=object), df['charge_type'].iloc[0] )#存資料
    print(datetime.datetime.now()-start_time)
if __name__ == "__main__":
    main()