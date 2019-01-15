
# coding: utf-8

import numpy as np
import pandas as pd
from sklearn.externals import joblib
from keras.models import load_model

FOLDER_PATH = '/storage/opt_project_test/optProjectTest/optProjectTest/models/cpc_48/'
MODEL_PATH = FOLDER_PATH + 'cpc_predict_20_20_32_.h5'
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

import mysql_save_load
import opt_minutes
import selection

# In[2]:


#init facebook api
my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

#target 

#ad_account_id = 'act_1587964461298459'
#ad_account_id = 'act_516689492098932'
ad_account_id = 'act_1910061912631003'
ad_campaign_id = '23842866254190246'
ad_id = '23843099735200492'
#
HOUR_TIME = 'hourly_stats_aggregated_by_audience_time_zone'

fields_ad = [ 'campaign_name','campaign_id','adset_name', 'adset_id', 'ad_name','ad_id', 'date_start' , 'cpm' , 'cpc' , 'clicks', 'impressions', 'reach', 'spend' ]
fields_adSet = [ AdSet.Field.id, AdSet.Field.start_time, AdSet.Field.end_time, AdSet.Field.bid_amount, AdSet.Field.daily_budget, AdSet.Field.budget_remaining, AdSet.Field.optimization_goal, AdSet.Field.targeting ]
fields_camp=[ Campaign.Field.spend_cap, Campaign.Field.start_time, Campaign.Field.stop_time, Campaign.Field.objective]
##### Campaign Target #####
COL_SPEND_CAP, COL_OBJECTIVE, COL_START_TIME, COL_STOP_TIME = 'spend_cap', 'objective', 'start_time', 'stop_time'
##### AdSet Target #####
COL_BID_AMOUNT, COL_DAILY_BUDGET, COL_AGE_MAX, COL_AGE_MIN = 'bid_amount', 'daily_budget', 'age_max', 'age_min'
COL_FLEXIBLE_SPEC, COL_GEO_LOCATIONS = 'flexible_spec', 'geo_locations'
##### Ad Target #####
COL_AD_ID, COL_CPC, COL_CLICKS, COL_IMPRESSIONS, COL_REACH = 'ad_id', 'cpc', 'clicks', 'impressions', 'reach'
COL_CAMPAIGN_ID, COL_ADSET_ID = 'campaign_id', 'adset_id'

PRED_CPC = 'pred_cpc'
PRED_BUDGET = 'pred_budget'
DECIDE_TYPE = 'optimization_type'
REASONS = 'reason'
STATUS = 'status'

ad_targets, ad_input = ['ad_id','cpc','clicks','impressions','reach','campaign_id','adset_id'], []
breakdowns = [HOUR_TIME]
ad_attribues = fields_ad + breakdowns

ad_file_name = 'ad_data/' + ad_account_id + '_ad.csv'
adset_file_name = 'ad_data/' + ad_account_id + '_adset.csv'
one_ad_file_name = 'ad_data/' + ad_id + '_ad.csv'

HOUR_PER_DAY = 18

# In[3]:


def checkFile(file_path, columns):
    if not file_path.exists():
        output_file = open(file_path, 'a+')
        #create column header
        for field in columns:
            output_file.write(field)
            
            if field != columns[-1]:
                output_file.write(',')
                
        output_file.write('\n')


def generateCSVForInsight(data, file_path, columns):
    
    checkFile(file_path , columns )
    output_file = open(file_path, 'a+')
    
    #create ad content for each row    
    for record in data:
        for field in columns:

            if record.get(field):
                field_value = record.get(field)                
                if field == HOUR_TIME:
                    field_value = field_value[0:2]                    
                output_file.write(field_value)
            else:
                output_file.write("None")
                
            if field != columns[-1]:
                output_file.write(",")
        output_file.write("\n")
    output_file.close()
        

    
def generateCSV_ForData(data, file_path, columns):
    
    checkFile(file_path , columns )
    output_file = open(file_path, 'a+')
    
    #create ad content for each row
    
    for field in columns:
        if data.get(field):
            field_value = data.get(field)                                  
            output_file.write(str(field_value))
        else:
            output_file.write("None")
        
        if field != columns[-1]:
            output_file.write(",")
    output_file.write("\n")
    output_file.close()
################  By Account  ################


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

def getFeatureByCampaign(ad_campaign_id):
    print('ad_campaign_id:' , ad_campaign_id)
    #ad_sets , get bid info from ad_set
    ad_campaign = Campaign( ad_campaign_id )
    adcamps = ad_campaign.remote_read( fields=fields_camp )
    spend_cap, objective = adcamps.get( COL_SPEND_CAP ), adcamps.get( COL_OBJECTIVE )
    start_time, stop_time = adcamps.get( COL_START_TIME ), adcamps.get( COL_STOP_TIME )
    stop_time = check_stop_time(stop_time)
    
    return spend_cap, objective, start_time, stop_time

def selectAdIDbyCampaign(ad_campaign_id):
    print('ad_campaign_id:' , ad_campaign_id)
    ad_campaign = Campaign(ad_campaign_id)
    ad_sets = ad_campaign.get_ad_sets(fields=[
        AdSet.Field.id
    ])
    mydict = {}
    for ad_set in ad_sets:
        adset_id = ad_set.get(AdSet.Field.id)
        adset = AdSet(adset_id)
        ads = adset.get_ads( fields=[Ad.Field.id] )
        for ad in ads:
            ad_id = ad.get(Ad.Field.id)
            print(['checking status'])
            print('ad_id is : ', ad_id, 'belongs to adset: ', adset_id)
            status = selection.performance_compute( adset_id, ad_id )
            pred_cpc, pred_budget, decide_type, reasons = getInsightByAd_Today( ad_id )
            mydict[ad_id] = { PRED_CPC: pred_cpc, PRED_BUDGET: pred_budget,
                                   DECIDE_TYPE: decide_type, REASONS: reasons, 
                                   STATUS: status }
            
            
#             mydict[ad_id] = status
            mysql_save_load.insertSelection( ad_campaign_id, adset_id, ad_id, status )
    return mydict

def get_total_daily_budget_by_campaign(camp_id):
    camp = Campaign(camp_id)
    camp_ads = camp.get_ad_sets(fields = [AdSet.Field.id])
    dailybudget_list = []
    for adset in camp_ads:
        adset_id = adset.get(AdSet.Field.id)
        adsets = AdSet(adset_id)
        adset_datas = adsets.remote_read( fields = [AdSet.Field.effective_status] )
        effective_status = adset_datas.get('effective_status')
        if effective_status == 'ACTIVE':
            _,daily_budget,_,_,_,_ = getInsightByAdset(adset_id)
            dailybudget_list.append(int(daily_budget))
    total_daily_budget = np.sum(dailybudget_list)
    return total_daily_budget

################  By AdSet  ################
def getInsightByAdset(ad_set_id):
    print('=====[getInsightByAdset]=====')
    print('ad_set_id:' , ad_set_id)
    ad_sets = AdSet(ad_set_id)
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
    print('ad_id:' , ad_id)
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
        
        mysql_save_load.insertData( ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations, total_clicks )
        
        if check_ad_input(ad_input):
        
            print('start optimization')
            next_cpc, next_budget, decide_type, reasons = opt_minutes.decide_strategy( int(ad_id), float(cpc), int(clicks), int(impressions), int(reach), int(bid_amount), int(daily_budget) )

            return next_cpc, next_budget, decide_type, reasons
        else:
            return 
    else:
        print('ad_id:',ad_id,'not delivering')
        
def getInsightByAd_Today(ad_id):
    print('[getInsightByAd_Today]')
    print('ad_id:' , ad_id)
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
                ad_input.extend( [bid_amount, daily_budget] )
            else:
                ad_input.append( ad_insight.get(field) )
    if len(ad_input) == 0:
        print('ad_id:', ad_id, 'not delivering')
        return None, None, None, ('ad not delivering')
    else:
        print('start making decisions')
        ad_id, cpc, clicks, impressions, reach, bid_amount, daily_budget = ad_input[0], ad_input[1], ad_input[2], ad_input[3], ad_input[4], ad_input[6], ad_input[7]
        next_cpc, next_budget, decide_type, reasons = opt_minutes.decide_strategy( int(ad_id),float(cpc),int(clicks),int(impressions),int(reach),int(bid_amount), int(daily_budget) )
        return next_cpc, next_budget, decide_type, reasons
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
    adset_count=0
    for adset in adsets:
        adset_count+=1
    avgspeed = TOTAL_CLICKS / adset_count / campaign_days / HOUR_PER_DAY
    avgspeed = avgspeed.astype(float)
    return avgspeed

def check_ad_input(ad_input):
    print(ad_input)
    for ad in ad_input:
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
    if (request_time - start_time).days > 1:
        return True
    else:
        return False
            
def main():
    print(' __name__ == "__main__"')
    #getAccessToken()

    print('get campaign and total clicks')
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
#     getAdByCampaign(23842866254190246, 12000)
#     mydict = selectAdIDbyCampaign(23842866254190246)

    print( getInsightByAd_Today(23842869152840246) )

#     print('[getAdByCampaign]')
#     getAdByCampaign(23842866254190246)
#     if check_time_interval(23842866254190246):
#         print('[selectAdIDbyCampaign]')
#         selectAdIDbyCampaign(23842866254190246)
        
if __name__ == "__main__":
#     main()
    FacebookAdsApi.init( my_app_id, my_app_secret, my_access_token )
    get_totaldailybudget_by_camp(23842866254190246)
# else:
#     print(' __name__ !!!!!!!!!!= "__main__"')
#     import sys
#     sys.path.append('opt/codes/')
#     import mysql_save_load
#     import opt_minutes
#     import selection   
# getAvgSpeed(23842866254190246, 120000)
#     getInsightByAd_Today(ad_id)
#     getAdsetByCampaign(ad_campaign_id)
#     getAdByAccount(ad_account_id)
#     getAdsetByAccount(ad_account_id)

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


