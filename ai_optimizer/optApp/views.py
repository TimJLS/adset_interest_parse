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
from ai_optimizer.codes import gdn_db
from ai_optimizer.codes import gsn_db
from ai_optimizer.codes import gdn_datacollector
from ai_optimizer.codes import gsn_datacollector
from facebook_business.api import FacebookAdsApi
from facebook_datacollector import Campaigns
import facebook_datacollector
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
        print('request post is:',  request.POST)
        
        if account_id and campaign_id and destination and destination_type and ai_start_date and ai_stop_date and ai_spend_cap and ai_status:
            brief_dict = {
                'campaign_id': campaign_id,
                'destination': destination,
                'destination_type': destination_type,
                'ai_start_date': ai_start_date,
                'ai_stop_date': ai_stop_date,
                'ai_spend_cap': ai_spend_cap,
                'ai_status': ai_status,
                'destination_max': destination_max,
            }
            if media == 'Facebook' or not media:
                mydict = dict()
                permission.init_facebook_api(int(account_id))
                
                custom_conversion_id = custom_conversion_handler.get_conversion_id_by_compaign(campaign_id)
                brief_dict['account_id'] = account_id
                brief_dict['custom_conversion_id'] = custom_conversion_id
                brief_dict['charge_type'] = brief_dict.pop('destination_type')
                queue = mysql_adactivity_save.check_campaignid_target( **brief_dict )
                if queue:
                    campaign = Campaigns( int(campaign_id), destination_type )
                    campaign_dict = campaign.generate_campaign_info()
                    try:lifetime_target = campaign_dict['target']
                    except:lifetime_target=0
                    try:
                        target_left_dict = {
                            'target_left': int(destination) - int(lifetime_target)
                        }
                    except:
                        temp = mysql_adactivity_save.get_campaign_target_dict()
                        destination = temp[ int(campaign_id) ]
                        target_left_dict = {
                            'target_left': int(destination) - int(charge)
                        }
                    charge_dict = { 'charge_type': destination_type }
                    target_dict = { 'destination': int(destination) }
                    campaign_dict = {
                        **campaign_dict,
                        **charge_dict,
                        **target_dict,
                        **target_left_dict,
                    }
                    print('[campaign_dict] ', campaign_dict)
                    df_camp = pd.DataFrame( campaign_dict, index=[0] )
                    df_camp[df_camp.columns] = df_camp[df_camp.columns].apply(pd.to_numeric, errors='ignore')
                    mysql_adactivity_save.update_campaign_target(df_camp)
                    try:
                        mydict = mysql_adactivity_save.get_result( campaign_id )
                    except:
                        pass
                return JsonResponse( json.loads(str(mydict)), safe=False )

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
        elif campaign_id: # brief system not OK yet
            print(campaign_id)
            try:
                mydict = mysql_adactivity_save.get_result( campaign_id )
            except:
                mydict = '{}'
            return JsonResponse( json.loads(mydict), safe=False )
    else:
        return JsonResponse( {}, safe=False )
        


# In[4]:


#get_ipython().system("jupyter nbconvert --output-dir='/home/tim_su/ai_optimizer/opt/ai_optimizer/optApp/' --to script /home/tim_su/ai_optimizer/opt/ai_optimizer/ai_optimizer/codes/notebook/views.ipynb")


# In[ ]:




