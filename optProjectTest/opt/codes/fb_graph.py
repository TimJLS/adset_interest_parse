
# coding: utf-8
import sys
import numpy as np
import pandas as pd
from sklearn.externals import joblib
from keras.models import load_model

FOLDER_PATH = '/storage/opt_project_test/optProjectTest/optProjectTest/models/cpc_120/'
MODEL_PATH = FOLDER_PATH + 'cpc_20_500_64.h5'
# model = load_model(MODEL_PATH)

SCALER_X_PATH = FOLDER_PATH + 'scalerX.pkl'
SCALER_Y_PATH = FOLDER_PATH + 'scalerY.pkl'
scalerX = joblib.load(SCALER_X_PATH)
scalerY = joblib.load(SCALER_Y_PATH)

# In[1]:


from pathlib import Path
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adcreative import AdCreative
import os

import datetime
import calendar
from sklearn.preprocessing import StandardScaler

import mysql_adactivity_save
import optimizer
import selection
import json
# In[2]:


#init facebook api
my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
# my_access_token = 'EAANoD9I4obMBALxUs9MgFAb1d4LKjMWjd0hgZBT1rLLU9WMT2eMikP7odldin1sKk5E9sy3tNV7uoeZC4NZBKHIILYURXYYL3id6GojW8BmGAzKHzWwnNnPbESpElGfFlOMzVKnQZBeeY7yN3BWn1nwjL53HcZBiZAoqNoxbx4krAchhMNZCZAwJ'

my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

#target 

#ad_account_id = 'act_1587964461298459'
#ad_account_id = 'act_516689492098932'
ad_account_id = 'act_1910061912631003'
ad_campaign_id = '23842866254190246'
ad_id = '23843099735200492'
#
HOUR_TIME = 'hourly_stats_aggregated_by_audience_time_zone'

fields_ad = [ 'campaign_id', 'adset_id','ad_id', 'cost_per_inline_link_click' , 'inline_link_clicks', 'impressions', 'reach' ]
fields_adSet = [ AdSet.Field.id, AdSet.Field.start_time, AdSet.Field.end_time, AdSet.Field.bid_amount, AdSet.Field.daily_budget, AdSet.Field.budget_remaining, AdSet.Field.optimization_goal, AdSet.Field.targeting, AdSet.Field.effective_status ]
fields_camp=[ Campaign.Field.id, Campaign.Field.spend_cap, Campaign.Field.start_time, Campaign.Field.stop_time, Campaign.Field.objective]

##### Campaign Target #####
COL_SPEND_CAP, COL_OBJECTIVE, COL_START_TIME, COL_STOP_TIME = 'spend_cap', 'objective', 'start_time', 'stop_time'
##### AdSet Target #####
COL_OPTIMAL_GOAL, COL_BID_AMOUNT, COL_DAILY_BUDGET, COL_AGE_MAX, COL_AGE_MIN = 'optimization_goal', 'bid_amount', 'daily_budget', 'age_max', 'age_min'
COL_FLEXIBLE_SPEC, COL_GEO_LOCATIONS = 'flexible_spec', 'geo_locations'
COL_EFFECTIVE_STATUS = 'effective_status'
##### Ad Target #####
COL_AD_ID, COL_CPC, COL_CLICKS, COL_IMPRESSIONS, COL_REACH = 'ad_id', 'cost_per_inline_link_click', 'inline_link_clicks', 'impressions', 'reach'
COL_CAMPAIGN_ID, COL_ADSET_ID = 'campaign_id', 'adset_id'

PRED_CPC = 'pred_cpc'
PRED_BUDGET = 'pred_budget'
DECIDE_TYPE = 'optimization_type'
REASONS = 'reason'
STATUS = 'status'
ADSET = 'adset_id'

ad_targets, ad_input = ['ad_id','cost_per_inline_link_click','inline_link_clicks','impressions','reach','campaign_id','adset_id'], []
breakdowns = [HOUR_TIME]
ad_attribues = fields_ad + breakdowns
columns = ['ad_id', 'cpc', 'clicks', 'impressions', 'reach', 'campaign_id', 'spend_cap', 'objective', 'start_time', 'stop_time', 'adset_id', 'bid_amount', 'daily_budget', 'age_max', 'age_min', 'flexible_spec', 'geo_locations', 'request_time']


ad_file_name = 'ad_data/' + ad_account_id + '_ad.csv'
adset_file_name = 'ad_data/' + ad_account_id + '_adset.csv'
one_ad_file_name = 'ad_data/' + ad_id + '_ad.csv'

HOUR_PER_DAY = 18
BID_RANGE = 1

COL_CAMPAIGN = [ 'campaign_id','spend_cap', 'objective', 'start_time', 'stop_time' ]
FEATURE_CAMPAIGN = [ 'id','spend_cap', 'objective', 'start_time', 'stop_time' ]
FEATURE_ADSET = [COL_ADSET_ID, COL_OPTIMAL_GOAL, COL_BID_AMOUNT,COL_DAILY_BUDGET,COL_AGE_MAX,COL_AGE_MIN,COL_FLEXIBLE_SPEC,COL_GEO_LOCATIONS]
# In[3]:
class Campaigns(object):
    def __init__( self, campaign_id ):
        self.campaign_id = campaign_id
    def get_campaign_feature( self ):
        campaign_feature_dict=dict(  )
        ad_campaign = Campaign( self.campaign_id )
        adcamps = ad_campaign.remote_read( fields=fields_camp )
        for campaign_feature in FEATURE_CAMPAIGN:
            campaign_feature_dict[ campaign_feature ]=adcamps.get( campaign_feature )
        return campaign_feature_dict

    def get_adids( self ):
        ad_id_list=list()
        ad_campaign = Campaign( self.campaign_id )
        ads = ad_campaign.get_ads( fields = [ Ad.Field.id ])
        for ad in ads:
            ad_id_list.append( ad.get("id") )
        return ad_id_list

class AdSets(object):
    def __init__( self, adset_id ):
        self.adset_id = adset_id
    def get_adset_insights( self ):
        ad_set = AdSet( self.adset_id )
        params = {
            "fields" : ",".join(fields_ad),
            "breakdowns" : ",".join(breakdowns),
            "time_increment" : 1,
            'limit' : 10000,
        }
        adsets = ad_set.remote_read( fields = fields_adSet, params = params )
        optimal_goal = adsets.get( COL_OPTIMAL_GOAL )
        bid_amount, daily_budget = adsets.get( COL_BID_AMOUNT ), adsets.get( COL_DAILY_BUDGET )
        age_max = adsets.get( AdSet.Field.targeting ).get( COL_AGE_MAX )
        age_min = adsets.get( AdSet.Field.targeting ).get( COL_AGE_MIN )
        flexible_spec = str( adsets.get( AdSet.Field.targeting ).get( COL_FLEXIBLE_SPEC ) )
        geo_locations = str( adsets.get( AdSet.Field.targeting ).get( COL_GEO_LOCATIONS ) )
        adset_insights_keys=FEATURE_ADSET
        adset_insights_values=[ self.adset_id, optimal_goal, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations ]
        adset_insight_dict = dict(zip(adset_insights_keys, adset_insights_values))
        return adset_insight_dict

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
         "fields" : ",".join( fields_ad ),
         "date_preset" : 'today',
         'limit' : 10000, }
        ad_insights = ad.get_insights( params=params )
        for ad_insight in ad_insights:
            for field in fields_ad:
                insight.append( ad_insight.get(field) )
        insight_keys = fields_ad
        insight_values = insight
        insight_dict = dict( zip(insight_keys, insight_values) )
        return insight_dict
    def get_campaign_feature( self ):
        ad = Ads( self.ad_id )
        campaign = Campaigns( ad.get_camp_id() )
        return campaign.get_campaign_feature()
    def get_adset_insights( self ):
        ad = Ads( self.ad_id )
        adset = AdSets( ad.get_adset_id() )
        return adset.get_adset_insights()

def data_collect( campaign_id, total_clicks ):
    request_time = datetime.datetime.now()
    for ad in Campaigns( campaign_id ).get_adids():
        ad_insights_dict = Ads(ad).get_ad_insights()
        if len(ad_insights_dict) != 0:
            target_dict = {'total_clicks':total_clicks, 'request_time':request_time}
            camp_feature_dict = Ads(ad).get_campaign_feature()
            adset_insight_dict = Ads(ad).get_adset_insights()
            feature_dict = {**target_dict, **ad_insights_dict, **camp_feature_dict, **adset_insight_dict}
            
            feature_dict['campaign_id'] = feature_dict.pop('id')
            feature_dict['cpc'] = feature_dict.pop('cost_per_inline_link_click')
            feature_dict['clicks'] = feature_dict.pop('inline_link_clicks')
#             feature_dict['start_time'] = feature_dict.pop('date_start')
            df = pd.DataFrame(feature_dict, index=[0])
            df['start_time'] = datetime.datetime.strptime( df['start_time'].iloc[0],'%Y-%m-%dT%H:%M:%S+%f' ).strftime( '%Y-%m-%d %H:%M:%S' )
            df['stop_time'] = datetime.datetime.strptime( df['stop_time'].iloc[0],'%Y-%m-%dT%H:%M:%S+%f' ).strftime( '%Y-%m-%d %H:%M:%S' )
            mysql_adactivity_save.intoDB("by_campaign", df)
    return



def compute_prediction(campaign_id, total_clicks):
    adid_list = mysql_adactivity_save.get_ad_id(campaign_id)
    for ad_id in adid_list:
        df = mysql_adactivity_save.get_ad_data( ad_id )
        if df.empty == False:
            cpc = df['cpc'].iloc[0].astype(dtype=object)
            clicks = df['clicks'].iloc[0].astype(dtype=object)
            impressions = df['impressions'].iloc[0].astype(dtype=object)
            reach = df['reach'].iloc[0].astype(dtype=object)
            bid_amount = df['bid_amount'].iloc[0].astype(dtype=object)
            daily_budget = df['daily_budget'].iloc[0].astype(dtype=object)

            optimizer.decide_strategy(ad_id, cpc, clicks, impressions, reach, bid_amount, daily_budget)
        
    return

def target_allocation(campaign_id):
    total_daily_budget = get_total_daily_budget_by_campaign( campaign_id )
    total_lastday_clicks = get_total_lastday_clicks_by_campaign( campaign_id )
    adid_list = mysql_adactivity_save.get_ad_id(campaign_id)
    total_clicks = mysql_adactivity_save.get_total_clicks( campaign_id )
    avgspeed = get_target_speed( campaign_id, total_clicks )
    mydict = dict()
    for ad_id in adid_list:
        ad_id = ad_id.astype(dtype=object)
        lastday_clicks = mysql_adactivity_save.get_lastday_clicks(ad_id)
        mydict[ad_id]=lastday_clicks
    backdict, adid_list = clicks_sorting(campaign_id, mydict)

    for ad_id in adid_list:
        performance_ratio = backdict[ad_id] / total_lastday_clicks
        target_budget = total_daily_budget * performance_ratio
        target_speed =  avgspeed * performance_ratio
        if backdict[ad_id] == 0:
            target_budget = total_daily_budget / 2 / len( backdict )
            target_speed = avgspeed / 2 / len( backdict )
            total_daily_budget = total_daily_budget - target_budget
#         print(ad_id,total_daily_budget, performance_ratio, target_budget, target_speed, avgspeed)
        mysql_adactivity_save.update_init_budget( ad_id, target_budget )
        mysql_adactivity_save.update_avgspeed( ad_id, target_speed )
    return

def clicks_sorting(campaign_id,mydict):
    adid_list = mysql_adactivity_save.get_ad_id( campaign_id )
    total_lastday_clicks = get_total_lastday_clicks_by_campaign( campaign_id )

    items=mydict.items()
    backitems=[[v[1],v[0]] for v in items]    
    backitems.sort()
    backitems=[[v[1],v[0]] for v in backitems]
    adid_list = list()
    for v in backitems:
        adid_list.append(v[0])

    backdict=dict()
    
    for ad_id, lastday_clicks in backitems:
        backdict[ad_id]=lastday_clicks

    return backdict, adid_list



def select_adid_by_campaign(ad_campaign_id):
    mydb = mysql_adactivity_save.connectDB("ad_activity")
    df = pd.read_sql( "SELECT * FROM by_campaign where campaign_id = %s" %(ad_campaign_id), con=mydb)
    adset_list = df[COL_ADSET_ID].unique()
    mydict={}
    for adset_id in adset_list:
        ad_list = df[COL_AD_ID][df.adset_id == adset_id].unique()
        for ad_id in ad_list:
            status = selection.performance_compute( adset_id, ad_id )
            pred_cpc, pred_budget, decide_type = mysql_adactivity_save.get_pred_data( ad_id )
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
    mysql_adactivity_save.insert_result( ad_campaign_id, mydict_json, datetime.now() )
    return

def get_total_daily_budget_by_campaign(camp_id):
    adid_list = mysql_adactivity_save.get_ad_id(camp_id)
    dailybudget_list = []
    for ad_id in adid_list:
        daily_budget = mysql_adactivity_save.get_init_budget(ad_id)
        dailybudget_list.append( int(daily_budget) )
    total_daily_budget = np.sum(dailybudget_list).astype(dtype=object)
    return total_daily_budget

def get_total_lastday_clicks_by_campaign(camp_id):
    adid_list = mysql_adactivity_save.get_ad_id(camp_id)
    lastday_clicks_list = []
    for ad_id in adid_list:
        lastday_clicks = mysql_adactivity_save.get_lastday_clicks(ad_id)
        lastday_clicks_list.append( int(lastday_clicks) )
    total_lastday_clicks = np.sum( lastday_clicks_list ).astype(dtype=object)
    return total_lastday_clicks


############################################
def getAvgSpeed(ad_campaign_id, TOTAL_CLICKS):
    campaign_feature_dict = Campaigns(ad_campaign_id).get_campaign_feature()

    start_time = campaign_feature_dict[COL_START_TIME]
    stop_time = campaign_feature_dict[COL_STOP_TIME]
    
    start_time = datetime.datetime.strptime( start_time, '%Y-%m-%dT%H:%M:%S+%f' )
    stop_time = datetime.datetime.strptime( stop_time, '%Y-%m-%dT%H:%M:%S+%f' )
    
    campaign_days = ( stop_time - start_time ).days
    print( 'campaign days : ', ( stop_time - start_time ).days )
    avgspeed = int(TOTAL_CLICKS) / campaign_days / HOUR_PER_DAY
    return avgspeed

def get_target_speed(ad_campaign_id, TOTAL_CLICKS):
    campaign_feature_dict = Campaigns(ad_campaign_id).get_campaign_feature()
    
    start_time = campaign_feature_dict[ COL_START_TIME ]
    stop_time = campaign_feature_dict[ COL_STOP_TIME ]
    
    start_time = datetime.datetime.strptime( start_time, '%Y-%m-%dT%H:%M:%S+%f' )
    stop_time = datetime.datetime.strptime( stop_time, '%Y-%m-%dT%H:%M:%S+%f' )
    campaign_days = ( stop_time - start_time ).days
    print( 'campaign days : ', ( stop_time - start_time ).days )
    avgspeed = int(TOTAL_CLICKS) / campaign_days / HOUR_PER_DAY
    return avgspeed
    
def check_ad_input(ad_input):
#     print(ad_input)
    for ad in ad_input:
        if ad == None:
            return False
    return True

def check_stop_time(stop_time):
    if stop_time == None:
        stop_time = datetime.strftime( datetime.now(), '%Y-%m-%dT%H:%M:%S+%f' )
    return stop_time

def check_time_interval(ad_campaign_id):
    request_time = datetime.datetime.now()
    print(request_time)
    campaign_feature_dict = Campaigns(ad_campaign_id).get_campaign_feature()
    start_time = campaign_feature_dict[ COL_START_TIME ]
    start_time = datetime.datetime.strptime( start_time, '%Y-%m-%dT%H:%M:%S+%f' )
    
    print( request_time - start_time )
    if (request_time - start_time).days >= 1:
        return True
    else:
        return False

def normalized_sigmoid_fkt(center, width, progress):
    s= 1/(1+np.exp(width*(progress-center)))
    return s
    
def revive_bidding(ad_id):
    campaign_id = Ads(ad_id).get_camp_id()
        
    init_cpc = mysql_adactivity_save.get_init_bidamount(ad_id)
    TOTAL_CLICKS = mysql_adactivity_save.get_total_clicks(campaign_id)
    avgspeed = get_target_speed(campaign_id, TOTAL_CLICKS)
    speed = optimizer.compute_speed(ad_id)
    center = 0.5
    width = 10
    progress = (speed)/avgspeed
    
    
    next_cpc = init_cpc + BID_RANGE*init_cpc*( normalized_sigmoid_fkt(center, width, progress) - 0.5 )
    print('------------tanh------------')
    print(progress)
    print(init_cpc)
    print(next_cpc)
    return next_cpc

def make_default( campaign_id ):
    mydb = mysql_adactivity_save.connectDB( "ad_activity" )
    df = pd.read_sql( "SELECT * FROM by_campaign where campaign_id = %s" %( campaign_id ), con=mydb)
    adset_list = df[ COL_ADSET_ID ].unique()
    mydict=dict()
    for adset_id in adset_list:
        ad_list = df[ COL_AD_ID ][ df.adset_id == adset_id ].unique()
        for ad_id in ad_list:
            bid_amount = mysql_adactivity_save.get_init_bidamount( ad_id )
            daily_budget = mysql_adactivity_save.get_init_budget( ad_id )
            mydict[ str( ad_id ) ] = {PRED_CPC: str(bid_amount), PRED_BUDGET: str(daily_budget),
                                      REASONS: "Requirements not match",DECIDE_TYPE: "Learning",
                                      STATUS: True, ADSET: str(adset_id),
                                     }
    mydict_json = json.dumps(mydict)
    mysql_adactivity_save.insert_default( str( campaign_id ), mydict_json, datetime.datetime.now() )            
    return

def bid_adjust(campaign_id):
    ad_id_list = mysql_adactivity_save.get_ad_id( campaign_id )
    for ad_id in ad_id_list:
        avgspeed, speed, decide_type = mysql_adactivity_save.get_speed(ad_id)
        ad_id = ad_id.astype(dtype=object)
        bid = revive_bidding(ad_id)
        mysql_adactivity_save.update_bidcap(ad_id, bid)
    return

def main(parameter):
    print(datetime.datetime.now())
    #getAccessToken()

    campaignid_target_dict = mysql_adactivity_save.get_campaignid_target()
#     print(campaignid_target_dict)
    for campaign_id in campaignid_target_dict:
        target = campaignid_target_dict.get(campaign_id)
        if parameter[1] == 'get_fb_data':
            FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
            data_collect( campaign_id.astype(dtype=object), target.iloc[0].astype(dtype=object) )#存資料
            make_default( campaign_id.astype(dtype=object) )
        elif parameter[1] == 'compute_predict':
            FacebookAdsApi.init( my_app_id, my_app_secret, my_access_token )
            compute_prediction( campaign_id.astype(dtype=object), target.iloc[0].astype(dtype=object) )#計算價格
            select_adid_by_campaign( campaign_id.astype(dtype=object) )#決定開關
        elif parameter[1] == 'kpi_detect':
            FacebookAdsApi.init( my_app_id, my_app_secret, my_access_token )
            bid_adjust(campaign_id)
        elif parameter[1]== 'allocation':
            FacebookAdsApi.init( my_app_id, my_app_secret, my_access_token )
            target_allocation( campaign_id.astype(dtype=object) )
        

        
if __name__ == "__main__":
    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv[1]))
    parameter = sys.argv
    main(parameter)

#     FacebookAdsApi.init( my_app_id, my_app_secret, my_access_token )
#     revive_bidding(23843180432930316)
# #     data_collect(23843180432940316)
#     target_allocation(23843052139470246)
#     target_allocation(23843085681850632)
#     bid_adjust(23843180432940316)
#     make_default(23843180432940316)
#     revive_bidding(23843085681870632)

# In[ ]:
# def getByCampaign(campaign_id):
#     print('campaign_id:' , campaign_id)
#     campaign = Campaign(campaign_id)
#     params = {
#      "fields" : "name"       
#     }
#     ad_sets = campaign.get_ad_sets(params=params)
#     for ad_set in ad_sets:
#         print(ad_set['name'], ad_set['id'])
#         getByAdSet(ad_set['id'])
#     print("--------")
    

# def getByAdSet(adset_id):
#     print('adset_id:' , adset_id)
#     adset = AdSet(adset_id)
#     params = {
#      "fields" : "name"       
#     }
#     ads = adset.get_ads(params=params)
#     for ad in ads:
#         print(ad['name'], ad['id'])
        
#     getByAd(ad['id'])
#     print("===adset===")
    
# def getAccessToken():
#     client = facebook.GraphAPI()
#     print( client.get_app_access_token(  my_app_id, my_app_secret  ) )


