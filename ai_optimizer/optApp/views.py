#!/usr/bin/env python
# coding: utf-8

# In[3]:


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
from ai_optimizer.codes import database_controller
from ai_optimizer.codes import gdn_db
from ai_optimizer.codes import gsn_db
from ai_optimizer.codes import gdn_datacollector
from ai_optimizer.codes import gsn_datacollector
from facebook_business.api import FacebookAdsApi
from facebook_datacollector_test import Campaigns
import facebook_datacollector_test
import facebook_custom_conversion_handler as custom_conversion_handler
import datetime
import adgeek_permission as permission


# FOLDER_PATH = 'ai_optimizer/models/cpc_120/'
# MODEL_PATH = FOLDER_PATH + 'cpc_20_500_64.h5'
class Field(object):
    pred_cpc = 'pred_cpc'
    pred_budget = 'pred_budget'
    target_TYPE = 'target_type'
    reasons = 'reasons'

    ad_id = 'ad_id'
    account_id = 'account_id'
    campaign_id = 'campaign_id'
    destination = 'total_clicks'
    destination_max = 'destination_max'
    destination_type = 'destination_type'
    media = 'media'
    ai_start_date = 'ai_start_date'
    ai_stop_date = 'ai_stop_date'
    ai_spend_cap = 'ai_spend_cap'
    ai_status = 'ai_status'
    is_smart_spending = 'is_smart_spending'
    is_target_suggest = 'is_target_suggest'
    is_lookalike = 'is_lookalike'
    is_creative_opt = 'is_creative_opt'
    
ESSENTIAL_PARAMETERS = [
    Field.account_id, Field.campaign_id, Field.destination, Field.destination_type, Field.ai_start_date, Field.ai_stop_date,
    Field.ai_spend_cap, Field.ai_status, Field.is_smart_spending, Field.is_target_suggest, Field.is_lookalike, Field.is_creative_opt,
]
    
@csrf_exempt
def opt_api(request):
    
    if request.method == "POST":
    
        start_time = datetime.datetime.now()
        account_id = request.POST.get(Field.account_id)
        campaign_id = request.POST.get(Field.campaign_id)
        destination = request.POST.get(Field.destination)
        destination_max = request.POST.get(Field.destination_max)
        destination_type = request.POST.get(Field.destination_type)
        media = request.POST.get(Field.media)
        
        ai_status = request.POST.get(Field.ai_status)
        ai_start_date = request.POST.get(Field.ai_start_date)
        ai_stop_date = request.POST.get(Field.ai_stop_date)
        ai_spend_cap = request.POST.get(Field.ai_spend_cap)
        is_smart_spending = request.POST.get(Field.is_smart_spending)
        is_target_suggest = request.POST.get(Field.is_target_suggest)
        is_lookalike = request.POST.get(Field.is_lookalike)
        is_creative_opt = request.POST.get(Field.is_creative_opt)
        print('request post is:',  request.POST)
        db = database_controller.Database()
        if all (k in request.POST for k in ESSENTIAL_PARAMETERS):
            database_fb = database_controller.FB( db )
            brief_dict = {
                'campaign_id': campaign_id,
                'destination': destination,
                'destination_type': destination_type,
                'ai_start_date': ai_start_date,
                'ai_stop_date': ai_stop_date,
                'ai_spend_cap': ai_spend_cap,
                'ai_status': ai_status,
                'destination_max': destination_max,
                'is_smart_spending': is_smart_spending, 
                'is_target_suggest': is_target_suggest,
                'is_lookalike': is_lookalike,
                'is_creative_opt': is_creative_opt,
            }
            if media == 'Facebook':
                mydict = dict()
                permission.init_facebook_api(int(account_id))
                
                custom_conversion_id = custom_conversion_handler.get_conversion_id_by_compaign(campaign_id)
                brief_dict['account_id'] = account_id
                brief_dict['custom_conversion_id'] = custom_conversion_id.item() if custom_conversion_id else None
                brief_dict['charge_type'] = brief_dict['destination_type']
                database_fb.upsert("campaign_target", brief_dict)
                campaign = Campaigns( int(campaign_id) )
                campaign_dict = campaign.generate_info()
                campaign_dict['target'] = int( campaign_dict.pop('action') )
                campaign_dict.pop('desire')
                campaign_dict.pop('interest')
                campaign_dict.pop('awareness')
                campaign_dict['actual_metrics'] = str( campaign_dict.get('actual_metrics') )
                lifetime_target = 0 if campaign_dict.get('target') is None else campaign_dict.get('target')
                target_left_dict = {
                    'target_left': int(destination) - int(lifetime_target)
                }
                charge_type_dict = { 'charge_type': destination_type }
                destination_type_dict = { 'destination_type': destination_type }
                target_dict = { 'destination': int(destination) }
                campaign_dict = {
                    **campaign_dict,
                    **charge_type_dict,
                    **destination_type_dict,
                    **charge_type_dict,
                    **target_dict,
                    **target_left_dict,
                }
                print('[campaign_dict] ', campaign_dict)
                database_fb.upsert("campaign_target", campaign_dict)
                return JsonResponse( {}, safe=False )
            
            elif media == 'GDN' and account_id:
                brief_dict['account_id'] = account_id
                if not gdn_db.check_campaignid_target(**brief_dict):
                    return JsonResponse( {}, safe=False )
                else:
                    mydict = gdn_db.get_result( campaign_id ) #new version
                    return JsonResponse( json.loads(mydict), safe=False )
                
            elif media == 'GSN' and account_id:
                brief_dict['account_id'] = account_id
                if not gsn_db.check_campaignid_target(**brief_dict):
                    return JsonResponse( {}, safe=False )
                else:
                    mydict = gdn_db.get_result( campaign_id ) #new version
                    return JsonResponse( json.loads(mydict), safe=False )                
            return JsonResponse( {}, safe=False )
    else:
        return JsonResponse( {}, safe=False )
        


# In[4]:


#get_ipython().system("jupyter nbconvert --output-dir='/home/tim_su/ai_optimizer/opt/ai_optimizer/optApp/' --to script /home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/notebook/views.ipynb")


# In[ ]:




