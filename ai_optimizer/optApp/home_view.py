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
import database_controller as db_controller
import mysql_adactivity_save as mysql_saver
from ai_optimizer.codes import gdn_db
from ai_optimizer.codes import gsn_db
from ai_optimizer.codes import gdn_datacollector

import facebook_custom_conversion_handler as custom_conversion_handler
import facebook_business.adobjects.campaign as facebook_business_campaign
import facebook_business.adobjects.adaccount as facebook_business_adaccount
import facebook_currency_handler as currency_handler
import datetime


import adgeek_permission as permission


class Campaign_FB():
    def __init__(self, campaign_id, account_id):   
        permission.init_facebook_api(account_id)
        db = db_controller.Database()
        self.database_fb = db_controller.FB(db)
        self.token_name = permission.get_access_name_by_account(account_id)
        self.account_id = account_id
        self.campaign_id = campaign_id
        self.get_campaign_status()
        self.get_campaign_name()
        self.get_account_name()
        self.compute()
        self.ai_start_date = self.ai_start_date.strftime("%Y/%m/%d")
        self.ai_stop_date = self.ai_stop_date.strftime("%Y/%m/%d")
        self.currency = currency_handler.get_currency_by_campaign(campaign_id)

    def get_campaign_name(self):
        this_campaign = facebook_business_campaign.Campaign(self.campaign_id).remote_read(fields=["name"])
        self.name = this_campaign.get('name')
        
    def get_account_name(self):
        account_id_act = 'act_' + str(self.account_id)
        this_account = facebook_business_adaccount.AdAccount(account_id_act).remote_read(fields=["name"])
        self.account_name = this_account.get('name')

    def get_campaign_status(self):
        one_campaign_list = self.database_fb.get_one_campaign(self.campaign_id).to_dict('records')
        if len(one_campaign_list) != 0:
            campaign = one_campaign_list[0]
            self.charge_type = campaign.get('destination_type')
            self.destination = campaign.get('destination')
            self.destination_max = campaign.get('destination_max')
            self.ai_spend_cap = campaign.get('ai_spend_cap')
            self.current_target_count = campaign.get('target', 0)
            self.left_target_count = campaign.get('target_left', 0)
            self.current_total_spend = campaign.get('spend', 0)
            self.ai_start_date = campaign.get('ai_start_date')
            self.ai_stop_date = campaign.get('ai_stop_date')
        else:
            print('[get_campaign_status] error, len ==0')
            
    def compute(self):
        self.ai_period = (self.ai_stop_date - self.ai_start_date ).days + 1
        today = datetime.date.today()
        self.ai_left_days = (self.ai_stop_date - today ).days + 1
        self.ai_running_days = (today - self.ai_start_date ).days + 1

        self.ai_daily_budget = round(self.ai_spend_cap / self.ai_period, 2)
        self.left_money_can_spend = self.ai_spend_cap - self.current_total_spend        
        self.left_money_can_spend_per_day = self.left_money_can_spend / self.ai_left_days if self.ai_left_days>0 else 1
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
    def __init__(self, campaign_id, account_id):
        db = db_controller.Database()
        self.database_gdn = db_controller.GDN(db)
        self.adwords_client = permission.init_google_api(account_id)
        self.token_name = permission.get_access_name_by_account(account_id)
        self.campaign_id = campaign_id
        self.get_campaign_status()
        self.get_campaign_name()
        self.compute()
        self.ai_start_date = self.ai_start_date.strftime("%Y/%m/%d")
        self.ai_stop_date = self.ai_stop_date.strftime("%Y/%m/%d")

    def get_campaign_name(self):
        self.adwords_client.SetClientCustomerId(self.customer_id)
        adword_service = self.adwords_client.GetService('CampaignService', version='v201809')
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
        one_campaign_list = self.database_gdn.get_one_campaign(self.campaign_id).to_dict('records')
        if len(one_campaign_list) != 0:
            campaign = one_campaign_list[0]
            self.charge_type = campaign.get('destination_type')
            self.destination = campaign.get('destination')
            self.destination_max = campaign.get('destination_max')
            self.ai_spend_cap = campaign.get('ai_spend_cap')
            self.current_target_count = campaign.get('target', 0)
            self.left_target_count = campaign.get('target_left', 0)
            self.current_total_spend = campaign.get('spend', 0)
            self.ai_start_date = campaign.get('ai_start_date')
            self.ai_stop_date = campaign.get('ai_stop_date')
            self.customer_id = campaign.get('customer_id')
        else:
            print('[get_campaign_status] error, len ==0')
        
        
    def compute(self):
#         if currency == 'USD':
#             self.ai_spend_cap = self.ai_spend_cap / 100

        if self.current_total_spend is None:
            self.current_total_spend = 0
        if self.left_target_count is None:
            self.left_target_count = 0    
        if self.current_target_count is None:
            self.current_target_count = 0    
            
        self.ai_period = (self.ai_stop_date - self.ai_start_date ).days + 1
        today = datetime.date.today()
        self.ai_left_days = (self.ai_stop_date - today ).days + 1
        self.ai_running_days = (today - self.ai_start_date ).days + 1
        
        self.ai_daily_budget = round(self.ai_spend_cap / self.ai_period, 2)
        self.left_money_can_spend = self.ai_spend_cap - self.current_total_spend
        self.left_money_can_spend_per_day = self.left_money_can_spend / self.ai_left_days
        self.max_cpc_for_future = self.left_money_can_spend / self.left_target_count if self.left_target_count>0 else self.left_money_can_spend
        self.kpi_cpc = round(self.ai_spend_cap / self.destination, 2)
        self.current_cpc =  round(self.current_total_spend / self.current_target_count, 2) if self.current_target_count !=0 else 0
        self.max_percent_arise_for_future = self.max_cpc_for_future / self.kpi_cpc

        self.destination_count_until_today = round(self.destination * (self.ai_running_days / self.ai_period) , 2)
        self.destination_speed_ratio = (self.current_target_count / self.destination_count_until_today) if self.destination_count_until_today != 0 else 0
        
        self.spend_until_today = round(self.ai_spend_cap * (self.ai_running_days / self.ai_period) , 2)
        self.spend_speed_ratio = (self.current_total_spend / self.spend_until_today) if self.spend_until_today != 0 else 0

class Campaign_GSN():
    def __init__(self, campaign_id, account_id):
        db = db_controller.Database()
        self.database_gsn = db_controller.GSN(db)
        self.adwords_client = permission.init_google_api(account_id)       
        self.token_name = permission.get_access_name_by_account(account_id)
        self.campaign_id = campaign_id
        self.get_campaign_status()
        self.get_campaign_name()
        self.compute()
        self.ai_start_date = self.ai_start_date.strftime("%Y/%m/%d")
        self.ai_stop_date = self.ai_stop_date.strftime("%Y/%m/%d")

    def get_campaign_name(self):
        self.adwords_client.SetClientCustomerId(self.customer_id)
        adword_service = self.adwords_client.GetService('CampaignService', version='v201809')
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
        one_campaign_list = self.database_gsn.get_one_campaign(self.campaign_id).to_dict('records')
        if len(one_campaign_list) != 0:
            campaign = one_campaign_list[0]
            self.charge_type = campaign.get('destination_type')
            self.destination = campaign.get('destination')
            self.destination_max = campaign.get('destination_max')
            self.ai_spend_cap = campaign.get('ai_spend_cap')
            self.current_target_count = campaign.get('target', 0)
            self.left_target_count = campaign.get('target_left', 0)
            self.current_total_spend = campaign.get('spend', 0)
            self.ai_start_date = campaign.get('ai_start_date')
            self.ai_stop_date = campaign.get('ai_stop_date')
            self.customer_id = campaign.get('customer_id')
        else:
            print('[get_campaign_status] error, len ==0')

    def compute(self):
#         if currency == 'USD':
#             self.ai_spend_cap = self.ai_spend_cap / 100

        self.ai_period = (self.ai_stop_date - self.ai_start_date ).days + 1
        today = datetime.date.today()
        self.ai_left_days = (self.ai_stop_date - today ).days + 1
        self.ai_running_days = (today - self.ai_start_date ).days + 1

        self.ai_daily_budget = round(self.ai_spend_cap / self.ai_period,2)
        if self.current_total_spend is None:
            self.current_total_spend = 0
        if self.left_target_count is None:
            self.left_target_count = 0    
        if self.current_target_count is None:
            self.current_target_count = 0    
            
        self.left_money_can_spend = self.ai_spend_cap - self.current_total_spend
        self.left_money_can_spend_per_day = self.left_money_can_spend / self.ai_left_days
        self.max_cpc_for_future = self.left_money_can_spend / self.left_target_count if self.left_target_count>0 else self.left_money_can_spend
        self.kpi_cpc = round(self.ai_spend_cap / self.destination, 2)
        self.current_cpc =  round(self.current_total_spend / self.current_target_count, 2) if self.current_target_count !=0 else 0
        self.max_percent_arise_for_future = self.max_cpc_for_future / self.kpi_cpc

        self.destination_count_until_today = round(self.destination * (self.ai_running_days / self.ai_period) , 2)
        self.destination_speed_ratio = (self.current_target_count / self.destination_count_until_today) if self.destination_count_until_today != 0 else 0
        
        self.spend_until_today = round(self.ai_spend_cap * (self.ai_running_days / self.ai_period) , 2)
        self.spend_speed_ratio = (self.current_total_spend / self.spend_until_today) if self.spend_until_today != 0 else 0
            
    
def get_fb_branding_campaign(db):
    database_fb = db_controller.FB(db)
    df_branding = database_fb.get_branding_campaign()
    campaign_list = []
    for index, row in df_branding.iterrows():
        campaign_id = row['campaign_id']
        account_id = row['account_id']
        c = Campaign_FB(campaign_id, account_id)
        campaign_list.append(c)    
    return campaign_list

def get_fb_performance_campaign(db):
    database_fb = db_controller.FB(db)
    df_performance = database_fb.get_performance_campaign()
    
    campaign_list = []
    for index, row in df_performance.iterrows():
        campaign_id = row['campaign_id']
        account_id = row['account_id']
        c = Campaign_FB(campaign_id, account_id)
        campaign_list.append(c)    
    return campaign_list

def get_gdn_campaign(db):
    database_gdn = db_controller.GDN(db)
    df_branding = database_gdn.get_running_campaign()
    campaign_list = []
    for index, row in df_branding.iterrows():
        account_id = row['customer_id']
        campaign_id = row['campaign_id']
        c = Campaign_GDN(campaign_id, account_id)
        campaign_list.append(c)    
    return campaign_list

def get_gsn_campaign():    
    df_branding = gsn_db.get_campaign_is_running()
    campaign_list = []
    for index, row in df_branding.iterrows():
        account_id = row['customer_id']
        campaign_id = row['campaign_id']
        c = Campaign_GSN(campaign_id, account_id)
        campaign_list.append(c)    
    return campaign_list

def compute_total_budget(campaign_list):
    total_budget = 0
    for campaign in campaign_list:
        total_budget += campaign.ai_spend_cap
    return total_budget
    
@csrf_exempt
def home_page(request):
#     return HttpResponse("Hello World!")
    
    db = db_controller.Database()
    campaign_fb_branding_list = get_fb_branding_campaign(db)
    campaign_fb_performance_list = get_fb_performance_campaign(db)
    campaign_gdn_list = get_gdn_campaign(db)
    campaign_gsn_list = get_gsn_campaign()
    
    budget_dic = {}
    budget_dic['fb_branding'] = compute_total_budget(campaign_fb_branding_list)
    budget_dic['fb_performance'] = compute_total_budget(campaign_fb_performance_list)
    budget_dic['gdn'] = compute_total_budget(campaign_gdn_list)
    budget_dic['gsn'] = compute_total_budget(campaign_gsn_list)
    
    return render(request, 'home_template.html', {'campaign_fb_branding_list': campaign_fb_branding_list, 'campaign_fb_performance_list': campaign_fb_performance_list, 'campaign_gdn_list': campaign_gdn_list, 'campaign_gsn_list': campaign_gsn_list, 'budget_dic': budget_dic })



