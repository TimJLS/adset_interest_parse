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
import mysql_adactivity_save as mysql_saver
from ai_optimizer.codes import gdn_db
from ai_optimizer.codes import gsn_db
from ai_optimizer.codes import gdn_datacollector

import facebook_custom_conversion_handler as custom_conversion_handler
import facebook_business.adobjects.campaign as facebook_business_campaign
import datetime

##FB
my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBALrHTgMWgRujnWcZA3ZB823phs6ynDDtQxnzIZASyRQZCHfr5soXBZA7NM9Dc4j9O8FtnlIzxiPCsYt4tmPQ6ZAT3yJLPuYQqjnWZBWX5dsOVzNhEqsHYj1jVJ3RAVVueW7RSxRDbNXKvK3W23dcAjNMjxIjQGIOgZDZD'

##GOOGLE
from googleads import adwords
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)


class Campaign_FB():
    def __init__(self, campaign_id):
        self.campaign_id = campaign_id
        self.get_campaign_status()
        self.get_campaign_name()
        self.compute()
        self.ai_start_date = self.ai_start_date.strftime("%Y/%m/%d")
        self.ai_stop_date = self.ai_stop_date.strftime("%Y/%m/%d")

    def get_campaign_name(self):
        this_campaign = facebook_business_campaign.Campaign( self.campaign_id).remote_read(fields=["name"])
        self.name = this_campaign.get('name')

    def get_campaign_status(self):
        my_db = mysql_saver.connectDB(mysql_saver.DATABASE)
        my_cursor = my_db.cursor()
        sql = 'SELECT charge_type, destination, destination_max, ai_spend_cap, target, target_left, spend , ai_start_date, ai_stop_date FROM campaign_target where campaign_id = {}'.format(self.campaign_id)
        my_cursor.execute(sql)
        self.charge_type, self.destination, self.destination_max, self.ai_spend_cap, self.current_target_count, self.left_target_count, self.current_total_spend, self.ai_start_date, self.ai_stop_date  = my_cursor.fetchone()
        
        my_db.commit()
        my_db.close()

    def compute(self):
#         if currency == 'USD':
#             self.ai_spend_cap = self.ai_spend_cap / 100

        self.ai_period = (self.ai_stop_date - self.ai_start_date ).days + 1
        today = datetime.date.today()
        self.ai_left_days = (self.ai_stop_date - today ).days + 1
        self.ai_running_days = (today - self.ai_start_date ).days + 1

        self.ai_daily_budget = self.ai_spend_cap / self.ai_period
        self.left_money_can_spend = self.ai_spend_cap - self.current_total_spend
        self.left_money_can_spend_per_day = self.left_money_can_spend / self.ai_left_days
        self.max_cpc_for_future = self.left_money_can_spend / self.left_target_count if self.left_target_count>0 else self.left_money_can_spend
        self.kpi_cpc = round(self.ai_spend_cap / self.destination, 2)
        self.current_cpc =  round(self.current_total_spend / self.current_target_count, 2) if self.current_target_count !=0 else 0
        self.max_percent_arise_for_future = self.max_cpc_for_future / self.kpi_cpc

        self.destination_count_until_today = round(self.destination * (self.ai_running_days / self.ai_period) , 2)
        if self.destination_count_until_today <=0:
            self.destination_count_until_today = 1
        self.destination_speed_ratio = self.current_target_count / self.destination_count_until_today
        
        self.spend_until_today = round(self.ai_spend_cap * (self.ai_running_days / self.ai_period) , 2)
        if self.spend_until_today <=0:
            self.spend_until_today = 1
        self.spend_speed_ratio = self.current_total_spend / self.spend_until_today


class Campaign_GDN():
    def __init__(self, campaign_id):
        self.campaign_id = campaign_id
        self.get_campaign_status()
        self.get_campaign_name()
        self.compute()
        self.ai_start_date = self.ai_start_date.strftime("%Y/%m/%d")
        self.ai_stop_date = self.ai_stop_date.strftime("%Y/%m/%d")


    def get_campaign_name(self):
        adwords_client.SetClientCustomerId(self.customer_id)
        adword_service = adwords_client.GetService('CampaignService', version='v201809')
        selector = [{
                'fields': 'Name',
                'predicates': [{
                            'field': 'CampaignId',
                            'operator': 'EQUALS',
                            'values': self.campaign_id
                        }]
            }]

        ad_params = adword_service.get(selector)
        if 'entries' in ad_params:
            for ad_dic in ad_params['entries']:
                self.name = ad_dic['name']


    def get_campaign_status(self):
        my_db = gdn_db.connectDB(gdn_db.DATABASE)
        my_cursor = my_db.cursor()
        sql = 'SELECT customer_id, destination_type, destination, ai_spend_cap, target, target_left, spend , ai_start_date, ai_stop_date FROM campaign_target where campaign_id = {}'.format(self.campaign_id)
        my_cursor.execute(sql)
        self.customer_id , self.charge_type, self.destination, self.ai_spend_cap, self.current_target_count, self.left_target_count, self.current_total_spend, self.ai_start_date, self.ai_stop_date  = my_cursor.fetchone()
        
        my_db.commit()
        my_db.close()

    def compute(self):
#         if currency == 'USD':
#             self.ai_spend_cap = self.ai_spend_cap / 100

        self.ai_period = (self.ai_stop_date - self.ai_start_date ).days + 1
        today = datetime.date.today()
        self.ai_left_days = (self.ai_stop_date - today ).days + 1
        self.ai_running_days = (today - self.ai_start_date ).days + 1

        self.ai_daily_budget = self.ai_spend_cap / self.ai_period
        self.left_money_can_spend = self.ai_spend_cap - self.current_total_spend
        self.left_money_can_spend_per_day = self.left_money_can_spend / self.ai_left_days
        self.max_cpc_for_future = self.left_money_can_spend / self.left_target_count if self.left_target_count>0 else self.left_money_can_spend
        self.kpi_cpc = round(self.ai_spend_cap / self.destination, 2)
        self.current_cpc =  round(self.current_total_spend / self.current_target_count, 2) if self.current_target_count !=0 else 0
        self.max_percent_arise_for_future = self.max_cpc_for_future / self.kpi_cpc

        self.destination_count_until_today = round(self.destination * (self.ai_running_days / self.ai_period) , 2)
        self.destination_speed_ratio = self.current_target_count / self.destination_count_until_today
        
        self.spend_until_today = round(self.ai_spend_cap * (self.ai_running_days / self.ai_period) , 2)
        self.spend_speed_ratio = self.current_total_spend / self.spend_until_today

class Campaign_GSN():
    def __init__(self, campaign_id):
        self.campaign_id = campaign_id
        self.get_campaign_status()
        self.get_campaign_name()
        self.compute()
        self.ai_start_date = self.ai_start_date.strftime("%Y/%m/%d")
        self.ai_stop_date = self.ai_stop_date.strftime("%Y/%m/%d")


    def get_campaign_name(self):
        adwords_client.SetClientCustomerId(self.customer_id)
        adword_service = adwords_client.GetService('CampaignService', version='v201809')
        selector = [{
                'fields': 'Name',
                'predicates': [{
                            'field': 'CampaignId',
                            'operator': 'EQUALS',
                            'values': self.campaign_id
                        }]
            }]

        ad_params = adword_service.get(selector)
        if 'entries' in ad_params:
            for ad_dic in ad_params['entries']:
                self.name = ad_dic['name']

    def get_campaign_status(self):
        my_db = gsn_db.connectDB(gsn_db.DATABASE)
        my_cursor = my_db.cursor()
        sql = 'SELECT customer_id, destination_type, destination, ai_spend_cap, target, target_left, spend , ai_start_date, ai_stop_date FROM campaign_target where campaign_id = {}'.format(self.campaign_id)
        my_cursor.execute(sql)
        self.customer_id, self.charge_type, self.destination, self.ai_spend_cap, self.current_target_count, self.left_target_count, self.current_total_spend, self.ai_start_date, self.ai_stop_date  = my_cursor.fetchone()
        
        my_db.commit()
        my_db.close()

    def compute(self):
#         if currency == 'USD':
#             self.ai_spend_cap = self.ai_spend_cap / 100

        self.ai_period = (self.ai_stop_date - self.ai_start_date ).days + 1
        today = datetime.date.today()
        self.ai_left_days = (self.ai_stop_date - today ).days + 1
        self.ai_running_days = (today - self.ai_start_date ).days + 1

        self.ai_daily_budget = self.ai_spend_cap / self.ai_period
        self.left_money_can_spend = self.ai_spend_cap - self.current_total_spend
        self.left_money_can_spend_per_day = self.left_money_can_spend / self.ai_left_days
        self.max_cpc_for_future = self.left_money_can_spend / self.left_target_count if self.left_target_count>0 else self.left_money_can_spend
        self.kpi_cpc = round(self.ai_spend_cap / self.destination, 2)
        self.current_cpc =  round(self.current_total_spend / self.current_target_count, 2) if self.current_target_count !=0 else 0
        self.max_percent_arise_for_future = self.max_cpc_for_future / self.kpi_cpc

        self.destination_count_until_today = round(self.destination * (self.ai_running_days / self.ai_period) , 2)
        self.destination_speed_ratio = self.current_target_count / self.destination_count_until_today
        
        self.spend_until_today = round(self.ai_spend_cap * (self.ai_running_days / self.ai_period) , 2)
        self.spend_speed_ratio = self.current_total_spend / self.spend_until_today
    
        
    
def get_fb_branding_campaign():    
    df_branding = mysql_saver.get_running_branding_campaign()
    campaign_list = []
    for index, row in df_branding.iterrows():
        campaign_id = row['campaign_id']
        c = Campaign_FB(campaign_id)
        campaign_list.append(c)    
    return campaign_list

def get_fb_performance_campaign():    
    df_branding = mysql_saver.get_running_conversion_campaign()
    campaign_list = []
    for index, row in df_branding.iterrows():
        campaign_id = row['campaign_id']
        c = Campaign_FB(campaign_id)
        campaign_list.append(c)    
    return campaign_list

def get_gdn_campaign():    
    df_branding = gdn_db.get_campaign_is_running()
    campaign_list = []
    for index, row in df_branding.iterrows():
        campaign_id = row['campaign_id']
        c = Campaign_GDN(campaign_id)
        campaign_list.append(c)    
    return campaign_list

def get_gsn_campaign():    
    df_branding = gsn_db.get_campaign_is_running()
    campaign_list = []
    for index, row in df_branding.iterrows():
        campaign_id = row['campaign_id']
        c = Campaign_GSN(campaign_id)
        campaign_list.append(c)    
    return campaign_list

@csrf_exempt
def home_page(request):
#     return HttpResponse("Hello World!")
    
    campaign_fb_branding_list = get_fb_branding_campaign()
    campaign_fb_performance_list = get_fb_performance_campaign()
    campaign_gdn_list = get_gdn_campaign()
    campaign_gsn_list = get_gsn_campaign()
    
    return render(request, 'home_template.html', {'campaign_fb_branding_list': campaign_fb_branding_list, 'campaign_fb_performance_list': campaign_fb_performance_list, 'campaign_gdn_list': campaign_gdn_list, 'campaign_gsn_list': campaign_gsn_list })



