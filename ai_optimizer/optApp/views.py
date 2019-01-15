from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import sys
sys.path.append('ai_optimizer/codes/')
import json
import numpy as np
import pandas as pd
from sklearn.externals import joblib

from ai_optimizer.codes import mysql_adactivity_save

from facebook_business.api import FacebookAdsApi
from facebook_datacollector import Campaigns
import facebook_datacollector
import datetime
# FOLDER_PATH = 'ai_optimizer/models/cpc_120/'
# MODEL_PATH = FOLDER_PATH + 'cpc_20_500_64.h5'

PRED_CPC = 'pred_cpc'
PRED_BUDGET = 'pred_budget'
DECIDE_TYPE = 'optimization_type'
REASONS = 'reasons'

AD_ID = 'ad_id'
CAMPAIGN_ID = 'campaign_id'
TARGET = 'target'
CHARGE_TYPE = 'charge_type'
MEDIA = 'media'

# SCALER_X_PATH = FOLDER_PATH + 'scalerX.pkl'
# SCALER_Y_PATH = FOLDER_PATH + 'scalerY.pkl'
# scalerX = joblib.load(SCALER_X_PATH)
# scalerY = joblib.load(SCALER_Y_PATH)

my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

@csrf_exempt
def opt_api(request):
    if request.method == "POST":
        campaign_id = request.POST.get(CAMPAIGN_ID)
        target = request.POST.get(TARGET)
        charge_type = request.POST.get(CHARGE_TYPE)
        media = request.POST.get(MEDIA)
        print(campaign_id, target, charge_type, media)
        if campaign_id and target and charge_type and media:
            if media == 'Facebook':
                FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
                queue = mysql_adactivity_save.check_campaignid_target( campaign_id, target, charge_type )
                facebook_datacollector.make_default( int(campaign_id) )
                if queue:
                    campaign_feature_dict = Campaigns( int(campaign_id) ).get_campaign_feature()
                    lifetime_target = facebook_datacollector.check_lifetime_target( campaign_id )
                    charge = lifetime_target[TARGET]
                    try:target_left_dict = {'target_left': int(target) - int(charge)}
                    except:
                        temp = mysql_adactivity_save.get_campaign_target_dict()
                        target = temp[int(campaign_id)]
                        target_left_dict = {'target_left': int(target) - int(charge)}
                    charge_dict = {'charge_type': charge_type}
                    target_dict = {'target': int(target)}
                    campaign_dict =  {**campaign_feature_dict, **charge_dict, **target_dict, **target_left_dict}
                    df_camp = pd.DataFrame(campaign_dict, index=[0])
                    try:
                        mydict = mysql_adactivity_save.get_result( campaign_id )
                    except:
                        mydict = mysql_adactivity_save.get_default( campaign_id )
                else:
                    mydict = mysql_adactivity_save.get_default( campaign_id )
                mydict = json.loads(mydict)
                return JsonResponse( mydict, safe=False )
        else:
            responseStr = '[POST] return json for adgeek_message is:'
            return  JsonResponse({'response': responseStr})
#         try:
#         except:
#             return  JsonResponse({'response': 'exception occur'})