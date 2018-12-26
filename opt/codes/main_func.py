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
import selection
import genetic_algorithm
LIMIT=10000
HOUR_PER_DAY = 20
BID_RANGE = 0.8
PRED_CPC, PRED_BUDGET, REASONS, DECIDE_TYPE, STATUS, ADSET = 'pred_cpc', 'pred_budget', 'reasons', 'decide_type', 'status', 'adset_id'
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
#             "breakdowns" : ",".join(breakdowns),
#             "time_increment" : 1,
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
        params = {"fields" : ",".join( fields_ad + field_clicks + insights_ad ),
                 "date_preset" : 'today',
                  'limit' : 10000, }
        ad_insights = ad.get_insights( params=params )
        ads = Ads( self.ad_id )
        target_type = Campaigns( ads.get_camp_id() ).get_campaign_feature()['target_type']
        charge_type = mysql_adactivity_save.get_campaign_target( ads.get_camp_id() )['charge_type'].iloc[0]        
        for ad_insight in ad_insights:
            for field in fields_ad:
                insight_dict[field]=ad_insight.get(field)
            if charge_type != 'CLICKS':         
                for field in insights_ad:
                    if field == 'actions' and ad_insight.get(field) != None:
                        for tar in ad_insight.get(field):
                            target = target_index[target_type]
                            charge = charge_index[charge_type]
                            if tar['action_type'] == target_index[target_type]:
                                insight_dict[COL_TARGET] = tar['value']
                                insight_dict[COL_CHARGE] = tar['value']
                    elif field == 'cost_per_action_type' and ad_insight.get(field) != None:
                        for tar in ad_insight.get(field):
                            target = target_index[target_type]
                            charge = charge_index[charge_type]                                                            
                            if tar['action_type'] == target_index[target_type]:
                                insight_dict[COL_TARGET_CPC] = tar['value']
                                insight_dict[COL_CHARGE_CPC] = tar['value']
                    elif ad_insight.get(field) == None:
                        insight_dict[COL_TARGET] = '0'
                        insight_dict[COL_CHARGE] = '0'
                        insight_dict[COL_TARGET_CPC] = '0'
                        insight_dict[COL_CHARGE_CPC] = '0'
            elif charge_type == 'CLICKS':
                for field in field_clicks:
                    if field == 'cpc':
                        if ad_insight.get(field) != None:
                            insight_dict[COL_CHARGE_CPC] = ad_insight.get(field)
                        else:
                            insight_dict[COL_CHARGE_CPC] = '0'
                    elif field == 'clicks':
                        insight_dict[COL_CHARGE] = ad_insight.get(field)
                for field in insights_ad:
                    if field == 'actions' and ad_insight.get(field) != None:
                        for tar in ad_insight.get(field):
                            target = target_index[target_type]
                            if tar['action_type'] == target_index[target_type]:
                                insight_dict[COL_TARGET] = tar['value']
                    elif field == 'cost_per_action_type' and ad_insight.get(field) != None:
                        for tar in ad_insight.get(field):
                            target = target_index[target_type]                                                        
                            if tar['action_type'] == target_index[target_type]:
                                insight_dict[COL_TARGET_CPC] = tar['value']
                    elif ad_insight.get(field) == None:
                        insight_dict[COL_TARGET] = '0'
                        insight_dict[COL_TARGET_CPC] = '0'
            return insight_dict
    def get_campaign_feature( self ):
        ad = Ads( self.ad_id )
        campaign = Campaigns( ad.get_camp_id() )
        return campaign.get_campaign_feature()
    def get_adset_insights( self ):
        ad = Ads( self.ad_id )
        adset = AdSets( ad.get_adset_id() )
        return adset.get_adset_insights()
######## Functions ########

def make_default( campaign_id ):
    mydb = mysql_adactivity_save.connectDB( "ad_activity" )
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

def normalized_sigmoid_fkt(center, width, progress):
    s= 1/(1+np.exp(width*(progress-center)))
    return s

def bid_adjust( campaign_id ):
    mydb = mysql_adactivity_save.connectDB("ad_activity")
    request_time = datetime.datetime.now()
    time_progress = ( request_time.hour + 1 ) / HOUR_PER_DAY

    df_camp = pd.read_sql( "SELECT * FROM campaign_target where campaign_id=%s" %( campaign_id ), con=mydb )
    df_ad = pd.read_sql( "SELECT * FROM ad_insights where campaign_id = %s ORDER BY request_time desc limit %s" %( campaign_id, LIMIT ), con=mydb )
    ad_id_list = df_ad['ad_id'].unique()
    result_dict=dict()
    adset_num = len( df_ad['adset_id'].unique() )
    campaign_days = ( df_camp['stop_time'].iloc[0] - df_camp['start_time'].iloc[0] ).days
    dfs = pd.DataFrame(columns=['adset_id', 'charge'])
    for ad_id in ad_id_list:
        ad_id = ad_id.astype(dtype=object)
        df_ad = pd.read_sql( "SELECT * FROM ad_insights where ad_id=%s ORDER BY request_time DESC LIMIT 1" %( ad_id ), con=mydb )
        adset_id = df_ad['adset_id'].iloc[0]

        df = selection.performance_sort( adset_id )
        dfs = pd.concat([dfs, df], axis=0, sort=False)
    dfs = dfs.sort_values(by=['charge'], ascending=False).reset_index(drop=True)
    campaign_performance = dfs['charge'].sum()
    campaign_target = df_camp['target_left'].iloc[0] / campaign_days
#     campaign_time_target = (campaign_target-campaign_performance) * time_progress
    campaign_time_target = campaign_target * time_progress
    adset_target = campaign_target / campaign_days / adset_num
    for ad_id in ad_id_list:
        ad_id = ad_id.astype(dtype=object)
        center = 1
        width = 5
        df_ad = pd.read_sql( "SELECT * FROM ad_insights where ad_id=%s ORDER BY request_time DESC LIMIT 1" %( ad_id ), con=mydb )
        adset_id = df_ad['adset_id'].iloc[0]
        
        adset_performance = df_ad['charge'].iloc[0]

        df_adset = pd.read_sql( "SELECT * FROM adset_insights where adset_id=%s LIMIT 1" %( adset_id ), con=mydb )
        init_cpc = df_adset['bid_amount'].iloc[0]
        adset_time_target = adset_target * time_progress
        adset_progress = adset_performance / adset_time_target
                
        if adset_performance > adset_time_target and campaign_performance < campaign_time_target:
            df_adset = pd.read_sql( "SELECT * FROM adset_insights where adset_id=%s ORDER BY request_time DESC LIMIT 1" %( adset_id ), con=mydb )
            bid = df_adset['bid_amount'].iloc[0].astype(dtype=object) 
        else:
            bid = init_cpc + BID_RANGE*init_cpc*( normalized_sigmoid_fkt(center, width, adset_progress) - 0.5 )
            bid = bid.astype(dtype=object)            
            if adset_progress > 1:
                bid = math.ceil(init_cpc) # Keep Initail Bid
#                 df_adset = pd.read_sql( "SELECT * FROM adset_insights where adset_id=%s ORDER BY request_time DESC LIMIT 1" %( adset_id ), con=mydb )
#                 bid = df_adset['bid_amount'].iloc[0].astype(dtype=object) 
#         print(campaign_id, ad_id, adset_performance, adset_time_target, adset_progress, campaign_performance, campaign_time_target, bid, init_cpc)
        print(campaign_id, ad_id, campaign_performance, campaign_time_target, campaign_target, time_progress, bid, init_cpc)
        ad_dict = {'ad_id':ad_id, 'request_time':datetime.datetime.now(), 'next_cpc':int(bid),
          PRED_CPC:bid, PRED_BUDGET: df_adset['daily_budget'].iloc[0].astype(dtype=object), DECIDE_TYPE: 'Revive' }
        df_ad = pd.DataFrame(ad_dict, index=[0])
        
        result_dict[str(ad_id)] = { PRED_CPC: int(bid), PRED_BUDGET: 10000, REASONS: "collecting data, settings no change.",
                               DECIDE_TYPE: 'Revive', STATUS: True, ADSET: str(adset_id) }
       
        table = 'pred'
        mysql_adactivity_save.intoDB(table, df_ad)
        mysql_adactivity_save.update_bidcap(ad_id, bid)
    mydict_json = json.dumps(result_dict)
    mysql_adactivity_save.insert_result( campaign_id, mydict_json, datetime.datetime.now() )
    return

def select_adid_by_campaign(ad_campaign_id):
    mydb = mysql_adactivity_save.connectDB("ad_activity")
    df = pd.read_sql( "SELECT * FROM ad_insights where campaign_id = %s" %(ad_campaign_id), con=mydb)
    adset_list = df['adset_id'].unique()
    mydict={}
    for adset_id in adset_list:
        ad_list = df['ad_id'][df.adset_id == adset_id].unique()
        for ad_id in ad_list:
            status = selection.performance_compute( adset_id, ad_id )
            pred_cpc, pred_budget, decide_type = mysql_adactivity_save.get_pred_data( ad_id )
            pred_budget = 10000
            if decide_type == 'KPI':
                reasons = "KPI haven't reached yet, start CPC optimize"
            elif decide_type == 'CPC':
                reasons = "KPI reached, start CPC optimize"
            elif decide_type == 'Revive':
                reasons = "Low performance."
            else:
                reasons = "collecting data, settings no change."
            mydict[str(ad_id)] = { PRED_CPC: pred_cpc, PRED_BUDGET: pred_budget, REASONS: reasons,
                              DECIDE_TYPE: decide_type, STATUS: status, ADSET: str(adset_id) }
            adset_id, ad_id = adset_id.astype(dtype=object), ad_id.astype(dtype=object)
            
            mysql_adactivity_save.insertSelection( ad_campaign_id, adset_id, ad_id, status )
    mydict_json = json.dumps(mydict)
    mysql_adactivity_save.insert_result( ad_campaign_id, mydict_json, datetime.datetime.now() )
    return

def check_lifetime_target( campaign_id ):
    insights = Campaigns( campaign_id ).get_lifetime_insights()
    df = mysql_adactivity_save.get_campaign_target( campaign_id )
    charge_type = df['charge_type'].iloc[0]

    charge = int( insights[0].get("clicks") )
#     campaign_target = df['target'].iloc[0] - charge
    campaign_lifetime=dict()
    campaign_lifetime['target']=charge
    return campaign_lifetime

def ga_optimal_weight(campaign_id):
    request_time = datetime.datetime.now().date()
    mydb = mysql_adactivity_save.connectDB( "ad_activity" )
    df_weight = pd.read_sql("SELECT * FROM optimal_weight WHERE campaign_id=%s " %(campaign_id), con=mydb)
    ad_id_list = Campaigns(campaign_id).get_adids()
    for ad_id in ad_id_list:
        df = genetic_algorithm.ObjectiveFunc.adset_status(ad_id)
        r = genetic_algorithm.ObjectiveFunc.adset_fitness( df_weight, df )

        df_ad=pd.read_sql("SELECT adset_id FROM ad_insights WHERE ad_id=%s LIMIT 1" %(ad_id), con=mydb)
        adset_id = df_ad['adset_id'].iloc[0].astype(dtype=object)              
        df_final = pd.DataFrame({'campaign_id':campaign_id, 'adset_id':adset_id, 'ad_id':ad_id, 'score':r, 'request_time':request_time}, index=[0])
        mysql_adactivity_save.intoDB("adset_score", df_final)
    return

def data_collect( campaign_id, total_clicks, charge_type ):
    request_time = datetime.datetime.now()
    request_dict = {'request_time': request_time}
    charge_dict = {'charge_type': charge_type}
    lifetime_target = check_lifetime_target( campaign_id )
    charge = lifetime_target['target']
    target_left_dict = {'target_left': int(total_clicks) - int(charge)}
    
    campaign_dict =  {**Campaigns( campaign_id ).get_campaign_feature(), **charge_dict, **target_left_dict}
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

def main(parameter):
    start_time = datetime.datetime.now()
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    campaignid_target_dict = mysql_adactivity_save.get_campaign_target_dict()
    for campaign_id in campaignid_target_dict:
        target = campaignid_target_dict.get(campaign_id)
        if parameter[1] == 'data_collect':
            print(datetime.datetime.now()-start_time)
            print(campaign_id)
            data_collect( campaign_id.astype(dtype=object), target.iloc[0].astype(dtype=object), 'CLICKS' )#存資料
            print(datetime.datetime.now()-start_time)
#             make_default( campaign_id.astype(dtype=object) )       
        elif parameter[1] == 'bid_adjust':
            bid_adjust( campaign_id.astype(dtype=object) )
#             select_adid_by_campaign( campaign_id.astype(dtype=object) )
        elif parameter[1] == 'ga_optimal_weight':
            ga_optimal_weight(campaign_id)
    print(datetime.datetime.now()-start_time)
    return

if __name__ == "__main__":
    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv[1]))
    parameter = sys.argv
    main(parameter)
