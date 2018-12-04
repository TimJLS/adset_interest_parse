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
from keras.models import load_model, model_from_json
from opt.codes import fb_graph
from opt.codes import mysql_adactivity_save
from opt.codes import optimizer
from opt.codes import selection

FOLDER_PATH = 'opt/models/cpc_120/'
MODEL_PATH = FOLDER_PATH + 'cpc_20_500_64.h5'

PRED_CPC = 'pred_cpc'
PRED_BUDGET = 'pred_budget'
DECIDE_TYPE = 'optimization_type'
REASONS = 'reasons'

AD_ID = 'ad_id'
CAMPAIGN_ID = 'campaign_id'
TOTAL_CLICKS = 'total_clicks'

SCALER_X_PATH = FOLDER_PATH + 'scalerX.pkl'
SCALER_Y_PATH = FOLDER_PATH + 'scalerY.pkl'
scalerX = joblib.load(SCALER_X_PATH)
scalerY = joblib.load(SCALER_Y_PATH)

my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

@csrf_exempt
def opt_api(request):
    if request.method == "POST":
        campaign_id = request.POST.get(CAMPAIGN_ID)
        total_clicks = request.POST.get(TOTAL_CLICKS)
        ad_id = request.POST.get(AD_ID)
        fb_graph.FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
        if campaign_id and total_clicks:
            ##########################if c_id not exist , save to campaign_table
            mysql_adactivity_save.check_campaignid_target( campaign_id, total_clicks )
            if fb_graph.check_time_interval( campaign_id ):
                mydict = mysql_adactivity_save.get_result( campaign_id )
                mydict = json.loads(mydict)
                return JsonResponse( mydict, safe=False )
            else:
                mydict = mysql_adactivity_save.get_default( campaign_id )
                mydict = json.loads(mydict)
                return JsonResponse( mydict, safe=False )


# def main():
#     opt_api()
# if __name__ == "__main__":
#     main()