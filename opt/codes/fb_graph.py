
# coding: utf-8
import sys
import numpy as np
import pandas as pd
from sklearn.externals import joblib
from keras.models import load_model
import math
FOLDER_PATH = '/storage/opt_project/opt/models/cpc_120/'
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

from datetime import datetime
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

fields_ad = [ 'campaign_name','campaign_id','adset_name', 'adset_id', 'ad_name','ad_id', 'date_start' , 'cpm' , 'cost_per_inline_link_click' , 'inline_link_clicks', 'impressions', 'reach', 'spend' ]
fields_adSet = [ AdSet.Field.id, AdSet.Field.start_time, AdSet.Field.end_time, AdSet.Field.bid_amount, AdSet.Field.daily_budget, AdSet.Field.budget_remaining, AdSet.Field.optimization_goal, AdSet.Field.targeting ]
fields_camp=[ Campaign.Field.spend_cap, Campaign.Field.start_time, Campaign.Field.stop_time, Campaign.Field.objective]
##### Campaign Target #####
COL_SPEND_CAP, COL_OBJECTIVE, COL_START_TIME, COL_STOP_TIME = 'spend_cap', 'objective', 'start_time', 'stop_time'
##### AdSet Target #####
COL_BID_AMOUNT, COL_DAILY_BUDGET, COL_AGE_MAX, COL_AGE_MIN = 'bid_amount', 'daily_budget', 'age_max', 'age_min'
COL_FLEXIBLE_SPEC, COL_GEO_LOCATIONS = 'flexible_spec', 'geo_locations'
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

ad_file_name = 'ad_data/' + ad_account_id + '_ad.csv'
adset_file_name = 'ad_data/' + ad_account_id + '_adset.csv'
one_ad_file_name = 'ad_data/' + ad_id + '_ad.csv'

HOUR_PER_DAY = 18

def getAdsetByAccount(ad_account_id):
    print('ad_account_id:', ad_account_id)  
      
    #ad_sets , get bid info from ad_set
    ad_account = AdAccount(ad_account_id)
    adsets = ad_account.get_ad_sets()
    for adset in adsets:
        getInsightByAd_Today(adset['id'])

################  By Campaign  ################

def getAdsetByCampaign(ad_campaign_id):
    print('ad_campaign_id:', ad_campaign_id)
    #ad_sets , get bid info from ad_set
    ad_campaign = Campaign(ad_account_id)
    adsets = ad_campaign.get_ads()
#     print(adsets)
    for adset in adsets:
        ad_id = adset.get("id")
        getInsightByAd_Today( ad_id )
    
def getAdByCampaign(ad_campaign_id, total_clicks):
    print('ad_campaign_id:' , ad_campaign_id)
    #ad_sets , get bid info from ad_set
    ad_campaign = Campaign(ad_campaign_id)
    ads = ad_campaign.get_ads(fields=[
        Ad.Field.id
    ])
    for ad in ads:
        ad_id = ad.get("id")
        getFeatureByAd(ad_id, total_clicks)

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
    if total_lastday_clicks == 0:
        total_lastday_clicks = 1
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

def getFeatureByCampaign(ad_campaign_id):
    print('ad_campaign_id:' , ad_campaign_id)
    #ad_sets , get bid info from ad_set
    ad_campaign = Campaign( ad_campaign_id )
    adcamps = ad_campaign.remote_read( fields=fields_camp )
    spend_cap, objective = adcamps.get( COL_SPEND_CAP ), adcamps.get( COL_OBJECTIVE )
    start_time, stop_time = adcamps.get( COL_START_TIME ), adcamps.get( COL_STOP_TIME )
    stop_time = check_stop_time(stop_time)
    
    return spend_cap, objective, start_time, stop_time

def select_adid_by_campaign(ad_campaign_id):

    mydb = mysql_adactivity_save.connectDB("ad_activity")
    df = pd.read_sql( "SELECT * FROM by_campaign where campaign_id = %s" %(ad_campaign_id), con=mydb)
    adset_list = df[COL_ADSET_ID].unique()
    mydict={}
    for adset_id in adset_list:
        ad_list = df[COL_AD_ID][df.adset_id == adset_id].unique()
        for ad_id in ad_list:
<<<<<<< HEAD
            adset_id, ad_id = adset_id.astype(dtype=object), ad_id.astype(dtype=object)
            status = selection.forOPT( adset_id, ad_id )
#             pred_cpc, pred_budget, decide_type = mysql_adactivity_save.get_pred_data( ad_id )
            pred_cpc = revive_bidding(ad_id)
            pred_budget = 10000

>>>>>>> 99d9fe34978fb78d0155703ef2693419869d888c
            decide_type = 'Revive'
            reasons = "Low Performance"
#             if decide_type == 'KPI':
#                 reasons = "KPI haven't reached yet, start KPI optimize"
#             elif decide_type == 'CPC':
#                 reasons = "KPI reached, start CPC optimize"
#             elif decide_type == 'Revive':
#                 reasons = "Low performance."
#             else:
#                 reasons = "collecting data, settings no change."
            mydict[str(ad_id)] = { PRED_CPC: math.ceil(pred_cpc), PRED_BUDGET: abs(pred_budget), REASONS: reasons,
                              DECIDE_TYPE: decide_type, STATUS: status, ADSET: str(adset_id) }
<<<<<<< HEAD
            ad_dict = {'ad_id':str(ad_id),
                      'request_time':datetime.now(),
                       'next_cpc':math.ceil(pred_cpc),
                      PRED_CPC:pred_cpc,
                      PRED_BUDGET: abs(pred_budget),
                      'decide_type': decide_type
                      }
#             print(ad_dict)
            df_ad = pd.DataFrame(ad_dict, index=[0])
#             print(ad_dict)
            table = 'pred'
            mysql_adactivity_save.intoDB(table, df_ad)
            
#             mysql_adactivity_save.insertSelection( ad_campaign_id, adset_id, ad_id, status )

>>>>>>> 99d9fe34978fb78d0155703ef2693419869d888c
    mydict_json = json.dumps(mydict)
    mysql_adactivity_save.insert_result( ad_campaign_id, mydict_json, datetime.now() )
    return

def get_total_daily_budget_by_campaign(camp_id):
    adid_list = mysql_adactivity_save.get_ad_id(camp_id)
    dailybudget_list = []
    for ad_id in adid_list:
        daily_budget = mysql_adactivity_save.get_init_budget(ad_id)
        dailybudget_list.append(int(daily_budget))
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

################  By AdSet  ################
def getInsightByAdset(ad_set_id):
#     print('=====[getInsightByAdset]=====')
#     print('ad_set_id:' , ad_set_id)
    ad_sets = AdSet(ad_set_id)
    params = {
     "fields" : ",".join(fields_ad)
     , "breakdowns" : ",".join(breakdowns)
     , "time_increment" : 1   
     , 'limit' : 10000
    }
    ad_set_datas = ad_sets.remote_read( fields = fields_adSet )
    bid_amount, daily_budget = ad_set_datas.get( COL_BID_AMOUNT ), ad_set_datas.get( COL_DAILY_BUDGET )
    age_max = ad_set_datas.get( AdSet.Field.targeting ).get( COL_AGE_MAX )
    age_min = ad_set_datas.get( AdSet.Field.targeting ).get( COL_AGE_MIN )
    flexible_spec = ad_set_datas.get( AdSet.Field.targeting ).get( COL_FLEXIBLE_SPEC )
    geo_locations = ad_set_datas.get( AdSet.Field.targeting ).get( COL_GEO_LOCATIONS )
    return bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations
#     generateCSV_ForData(ad_set_data, Path(adset_file_name) , fields_adSet )

################  By Ad  ################
def getCampIDbyAd(ad_id):
    ad = Ad(ad_id)
    campaign = ad.remote_read( fields=[ Ad.Field.campaign_id ] )
    return int( campaign.get(COL_CAMPAIGN_ID) )
def getFeatureByAd(ad_id, total_clicks):
#     print('ad_id:' , ad_id)
    #ads , get bid info from ad
    ad = Ad(ad_id)
    params = {
     "fields" : ",".join(fields_ad)
     , "date_preset" : 'today' 
     , 'limit' : 10000
    }
    ad_insights = ad.get_insights( params=params )
    ad_input=[]
    for ad_insight in ad_insights:
        ad_input=[]
        for ad_target in ad_targets:
            if ad_target == COL_CPC and ad_insight.get(ad_target) == None:
                ad_input.append(0)
            elif ad_target == COL_CAMPAIGN_ID:
                ad_input.append( ad_insight.get(ad_target) )
                spend_cap, objective, start_time, stop_time = getFeatureByCampaign( ad_insight.get(ad_target) )
                start_time = datetime.strptime( start_time,'%Y-%m-%dT%H:%M:%S+%f' ).strftime( '%Y-%m-%d %H:%M:%S' )
                stop_time = datetime.strptime( stop_time,'%Y-%m-%dT%H:%M:%S+%f' ).strftime( '%Y-%m-%d %H:%M:%S' )
                ad_input.extend( [spend_cap, objective, start_time, stop_time] )
            elif ad_target == COL_ADSET_ID:
                ad_input.append( ad_insight.get(ad_target) )
                bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations = getInsightByAdset( ad_insight.get(ad_target) )
                ad_input.extend( [bid_amount, daily_budget, age_max, age_min, str(flexible_spec), str(geo_locations)] )
            else:
                ad_input.append( ad_insight.get(ad_target) )
    if len(ad_input) != 0:
        
        ad_id, cpc, clicks, impressions, reach = ad_input[0], ad_input[1], ad_input[2], ad_input[3], ad_input[4]
        
        campaign_id, spend_cap, objective, start_time, stop_time = ad_input[5], ad_input[6], ad_input[7], ad_input[8], ad_input[9]
        
        adset_id, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations = ad_input[10], ad_input[11], ad_input[12], ad_input[13], ad_input[14], ad_input[15], ad_input[16]
        
        mysql_adactivity_save.insertData( ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations, total_clicks )
        
    else:
#         print('ad_id:',ad_id,'not delivering')
        return
        
def getInsightByAd_Today(ad_id):
#     print('[getInsightByAd_Today]')
#     print('ad_id:' , ad_id)
    ad = Ad(ad_id)
    params = {
     "fields" : ",".join(fields_ad)
     , "date_preset" : 'today' 
     , 'limit' : 10000
    }
    ad_insights = ad.get_insights(params=params)
    ad_input=[]
    for ad_insight in ad_insights:
        ad_input=[]
        for field in ad_targets:
            if field == COL_CPC and ad_insight.get(field) == None:
                ad_input.append(0)
            elif field == COL_ADSET_ID:
                adset_id = ad_insight.get(field)
                bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations = getInsightByAdset(adset_id)
                ad_input.extend( [bid_amount, daily_budget, adset_id] )
            else:
                ad_input.append( ad_insight.get(field) )
    if len(ad_input) == 0:
#         print('ad_id:', ad_id, 'not delivering')
        return None, None, None, ('ad not delivering')
    else:
        print('start making decisions')
        ad_id, cpc, clicks, impressions, reach, bid_amount, daily_budget, adset_id = ad_input[0], ad_input[1], ad_input[2], ad_input[3], ad_input[4], ad_input[6], ad_input[7], ad_input[8]
        next_cpc, next_budget, decide_type, reasons = optimizer.decide_strategy( int(ad_id),float(cpc),int(clicks),int(impressions),int(reach),int(bid_amount), int(daily_budget) )
        return next_cpc, next_budget, decide_type, reasons, adset_id
    #    generateCSVForInsight_Today(ad_insights, Path(one_ad_file_name), fields_ad )    

############################################
def getAvgSpeed(ad_campaign_id, TOTAL_CLICKS):
    ad_campaign = Campaign(ad_campaign_id)
    adsets = ad_campaign.get_ad_sets( fields=[ AdSet.Field.id ] )
    spend_cap, objective, start_time, stop_time = getFeatureByCampaign(ad_campaign_id)
    start_time = datetime.strptime( start_time, '%Y-%m-%dT%H:%M:%S+%f' )
    stop_time = datetime.strptime( stop_time, '%Y-%m-%dT%H:%M:%S+%f' )
    campaign_days = ( stop_time - start_time ).days
    print( 'campaign days : ', ( stop_time - start_time ).days )
    avgspeed = int(TOTAL_CLICKS) / campaign_days / HOUR_PER_DAY
    return avgspeed

def get_target_speed(ad_campaign_id, TOTAL_CLICKS):
    ad_campaign = Campaign(ad_campaign_id)
    adsets = ad_campaign.get_ad_sets( fields=[ AdSet.Field.id ] )
    spend_cap, objective, start_time, stop_time = getFeatureByCampaign(ad_campaign_id)
    start_time = datetime.strptime( start_time, '%Y-%m-%dT%H:%M:%S+%f' )
    stop_time = datetime.strptime( stop_time, '%Y-%m-%dT%H:%M:%S+%f' )
    campaign_days = ( stop_time - start_time ).days
    print( 'campaign days : ', ( stop_time - start_time ).days )
    adset_count=0
    for adset in adsets:
        adset_count+=1
    target_speed = TOTAL_CLICKS / adset_count / campaign_days / HOUR_PER_DAY
    return target_speed

def check_ad_input(ad_input):
#     print(ad_input)
    ad_id, cpc, clicks, impressions, reach, bid_amount, daily_budget = ad_input[0],ad_input[1],ad_input[2],ad_input[3],ad_input[4],ad_input[11],ad_input[12]
    ad_input_list = [ad_id, cpc, clicks, impressions, reach, bid_amount, daily_budget]
    for ad in ad_input_list:
        if ad == None:
            return False
    return True

def check_stop_time(stop_time):
    if stop_time == None:
        stop_time = datetime.strftime( datetime.now(), '%Y-%m-%dT%H:%M:%S+%f' )
    return stop_time

def check_time_interval(ad_campaign_id):
    request_time = datetime.now()
    print(request_time)
    _, _, start_time, _ = getFeatureByCampaign(ad_campaign_id)
    start_time = datetime.strptime( start_time, '%Y-%m-%dT%H:%M:%S+%f' )
    
    print( request_time - start_time )
    if (request_time - start_time).days >= 1:
        return True
    else:
        return False

def normalized_sigmoid_fkt(a, b, x):
    '''
    Returns array of a horizontal mirrored normalized sigmoid function
    output between 0 and 1
    Function parameters a = center; b = width
    '''
    s= 1/(1+np.exp(b*(x-a)))
    return s
    
def revive_bidding(ad_id):
<<<<<<< HEAD
    campaign_id = mysql_adactivity_save.get_campaign_id(ad_id)
    init_cpc = mysql_adactivity_save.get_init_bidamount(ad_id)
    TOTAL_CLICKS = mysql_adactivity_save.get_total_clicks(campaign_id)
    avgspeed, speed, _ = mysql_adactivity_save.get_speed(ad_id)
    
    if avgspeed == 0:
        avgspeed = get_target_speed( campaign_id, TOTAL_CLICKS )
    mydb = mysql_adactivity_save.connectDB("ad_activity")
    df_camp = pd.read_sql("SELECT * FROM campaign_target where campaign_id=%s" % (campaign_id), con=mydb )
    avgspeed = df_camp['avgspeed'].iloc[0].astype(dtype=object)

>>>>>>> 99d9fe34978fb78d0155703ef2693419869d888c
    speed = optimizer.compute_speed(ad_id)
    center = 0.5
    width = 5
    progress = (speed)/avgspeed
    if progress > 0.5:
        next_cpc = math.ceil(init_cpc)
    else:
<<<<<<< HEAD
        next_cpc = init_cpc + BID_RANGE *( normalized_sigmoid_fkt(center, width, progress) - 0.5 )
#        next_cpc = init_cpc + BID_RANGE * init_cpc*( normalized_sigmoid_fkt(center, width, progress) - 0.5 )
        next_cpc = next_cpc.astype(dtype=object)
    
    mysql_adactivity_save.update_bidcap(ad_id, next_cpc)

>>>>>>> 99d9fe34978fb78d0155703ef2693419869d888c
    return next_cpc

def make_default( campaign_id ):
    mydb = mysql_adactivity_save.connectDB( "ad_activity" )
    df = pd.read_sql( "SELECT * FROM by_campaign where campaign_id = %s" %( ad_campaign_id ), con=mydb)
    adset_list = df[ COL_ADSET_ID ].unique()
    mydict=dict()
    for adset_id in adset_list:
        ad_list = df[ COL_AD_ID ][ df.adset_id == adset_id ].unique()
        for ad_id in ad_list:
            bid_amount = mysql_adactivity_save.get_init_bidamount( ad_id )
            daily_budget = mysql_adactivity_save.get_init_budget( ad_id )
            mydict[ str( ad_id ) ] = { PRED_CPC: str(bid_amount), PRED_BUDGET: str(daily_budget), REASONS: "Requirements not match",
                                  DECIDE_TYPE: "Learning", STATUS: True, ADSET: str(adset_id) }
    mydict_json = json.dumps(mydict)
    mysql_adactivity_save.insert_default( str( campaign_id ), mydict_json, datetime.now() )            
    return

def bid_adjust(campaign_id):
    ad_id_list = mysql_adactivity_save.get_ad_id( campaign_id )
    for ad_id in ad_id_list:
        avgspeed, speed, decide_type = mysql_adactivity_save.get_speed(ad_id)
        ad_id = ad_id.astype(dtype=object)
<<<<<<< HEAD

>>>>>>> 99d9fe34978fb78d0155703ef2693419869d888c
        bid = revive_bidding(ad_id)
        mysql_adactivity_save.update_bidcap(ad_id, bid)
    return

def main(parameter):
    print(datetime.now())
<<<<<<< HEAD

    campaignid_target_dict = mysql_adactivity_save.get_campaign_target_dict()
    FacebookAdsApi.init( my_app_id, my_app_secret, my_access_token )
    for campaign_id in campaignid_target_dict.keys():
        target = campaignid_target_dict.get(campaign_id)
        if parameter[1] == 'get_fb_data':
            getAdByCampaign( campaign_id.astype(dtype=object), target.iloc[0].astype(dtype=object) )#存資料
            make_default( campaign_id.astype(dtype=object) )
        elif parameter[1] == 'compute_predict':
            compute_prediction( campaign_id.astype(dtype=object), target.iloc[0].astype(dtype=object) )#計算價格
            select_adid_by_campaign( campaign_id.astype(dtype=object) )#決定開關
        elif parameter[1] == 'kpi_detect':
            select_adid_by_campaign( campaign_id.astype(dtype=object) )
        elif parameter[1]== 'allocation':
            target_allocation( campaign_id.astype(dtype=object) )
    return

>>>>>>> 99d9fe34978fb78d0155703ef2693419869d888c
        
if __name__ == "__main__":
    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv[1]))
    parameter = sys.argv
    main(parameter)
    

#     FacebookAdsApi.init( my_app_id, my_app_secret, my_access_token )
#     target_allocation(23843052139470246)


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


