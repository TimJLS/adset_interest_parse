# coding: utf-8
from django.http import HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import render
import sys
sys.path.append('opt/codes/')
import json
import numpy as np
import pandas as pd
from sklearn.externals import joblib

from opt.codes import mysql_adactivity_save

from facebook_business.api import FacebookAdsApi
from facebook_datacollector import Campaigns
import facebook_datacollector
import datetime
FOLDER_PATH = 'opt/models/cpc_120/'
MODEL_PATH = FOLDER_PATH + 'cpc_20_500_64.h5'

PRED_CPC = 'pred_cpc'
PRED_BUDGET = 'pred_budget'
DECIDE_TYPE = 'optimization_type'
REASONS = 'reasons'

AD_ID = 'ad_id'
CAMPAIGN_ID = 'campaign_id'
TARGET = 'target'
CHARGE_TYPE = 'charge_type'
MEDIA = 'media'

SCALER_X_PATH = FOLDER_PATH + 'scalerX.pkl'
SCALER_Y_PATH = FOLDER_PATH + 'scalerY.pkl'
scalerX = joblib.load(SCALER_X_PATH)
scalerY = joblib.load(SCALER_Y_PATH)

my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

@csrf_exempt
def opt_test_api(request):
    if request.method == "POST":
        campaign_id = request.POST.get(CAMPAIGN_ID)
        target = request.POST.get(TARGET)
        charge_type = request.POST.get(CHARGE_TYPE)
        media = request.POST.get(MEDIA)
        print(campaign_id)
        try:
            print(campaign_id and target and charge_type and media)
            if campaign_id and target and charge_type and media:
                if media == 'Facebook':
                    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
                    facebook_datacollector.make_default( int(campaign_id) )
                    queue = mysql_adactivity_save.check_campaignid_target( campaign_id, target, charge_type )
                    if queue:
                        mydict = mysql_adactivity_save.get_result( campaign_id )
                        mydict = json.loads(mydict)
                    else:
                        campaign_feature_dict = Campaigns( int(campaign_id) ).get_campaign_feature()
                        charge_dict = {'charge_type': charge_type}
                        target_dict = {'target': int(target)}
                        campaign_dict =  {**campaign_feature_dict, **charge_dict, **target_dict}
                        df_camp = pd.DataFrame(campaign_dict, index=[0])
                        mysql_adactivity_save.update_campaign_target(df_camp)
                        mydict = mysql_adactivity_save.get_default( campaign_id )
                        mydict = json.loads(mydict)
                    return JsonResponse( mydict, safe=False )
        except:
            pass
#             return JsonResponse( mydict, safe=False )

# def main():
#     opt_api()
# if __name__ == "__main__":
#     main()