#!/usr/bin/env python
# coding: utf-8

# In[1]:


import uuid
from googleads import adwords
import sys
import pandas as pd
import numpy as np
import math
import datetime
from bid_operator import reverse_bid_amount
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
import gdn_db
CAMPAIGN_OBJECTIVE_FIELD = {
    'LINK_CLICKS': 'clicks',
    'CONVERSIONS':'conversions',
    'cpc': 'clicks',
    'cpa':'conversions',
}

CAMPAIGN_FIELDS = ['ExternalCustomerId','CampaignId','AdvertisingChannelType', 'CampaignStatus',
                   'BiddingStrategyType','Amount','StartDate','EndDate','Cost',
                   'AverageCost','Impressions', 'Clicks','Conversions', 'AllConversions',
                   'AverageCpc','CostPerConversion', 'CostPerAllConversion', 'Ctr']
DB_CAMPAIGN_COLUMN_NAME_LIST = [
    'customer_id', 'campaign_id', 'channel_type', 'status', 'bidding_type', 'daily_budget', 'start_time', 'stop_time',
    'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click',
    'cost_per_conversion', 'cost_per_all_conversion', 'ctr'
]
BIDDING_INDEX = {
    'cpc': 'cpc_bid',
    'Target CPA': 'cpa_bid',
}

class ReportField:
    # URL
    URL_LIST = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'AdGroupStatus', 'Cost', 'AverageCost',
        'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'DisplayName', 'Ctr']
    # CRITERIA,
    CRITERIA_LIST = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'Criteria', 'Id', 'AdGroupStatus', 'CpmBid', 'CpcBid',
        'Cost', 'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'Ctr',]
    # AUDIENCE, AGE_RANGE, DISPLAY_KEYWORD
    BIDDABLE_LIST = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'Criteria', 'Id', 'AdGroupStatus', 'CpmBid', 'CpcBid',
        'BiddingStrategyType', 'Cost', 'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'Ctr']
    # ADGROUP
    ADGROUP_LIST = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupType', 'AdGroupId', 'AdGroupStatus', 'CpmBid', 'CpvBid', 'CpcBid', 'TargetCpa',
        'BiddingStrategyType', 'Cost', 'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'Ctr']
    # KEYWORDS
    KEYWORDS_LIST = [
        'ExternalCustomerId','CampaignId', 'AdGroupId', 'AdGroupStatus', 'Criteria', 'Id','AveragePosition', 'SystemServingStatus', 'FirstPageCpc',
        'CpmBid', 'CpcBid', 'BiddingStrategyType', 'AverageCost','Cost','Impressions', 'Clicks','Conversions', 'AllConversions', 'AverageCpc',
        'CostPerConversion', 'CostPerAllConversion', 'Ctr']
    
    NON_NUMERIC_LIST = [
        'Criterion serving status', 'Ad group type', 'Ad group state', 'Bid Strategy Type', 'Keyword', 'Age Range', 'Audience', 'Keyword / Placement', 'Criteria Display Name']

    NUMERIC_LIST = [
        'First page CPC', 'Max. CPM', 'Max. CPV', 'Max. CPC', 'Default max. CPC', 'Target CPA', 'Cost', 'Avg. Cost', 'Avg. CPC', 'Cost / conv.', 'Cost / all conv.']

    INDEX = {
        'ADGROUP': ADGROUP_LIST,
        'URL': URL_LIST,
        'CRITERIA': CRITERIA_LIST,
        'AUDIENCE': BIDDABLE_LIST,
        'AGE_RANGE': BIDDABLE_LIST,
        'DISPLAY_KEYWORD': BIDDABLE_LIST,
        'KEYWORDS': KEYWORDS_LIST,
    }
class ReportColumn:
    ADGROUP_LIST  = [
        'customer_id', 'campaign_id', 'channel_type', 'adgroup_id', 'status', 'cpm_bid', 'cpv_bid', 'cpc_bid', 'cpa_bid', 'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr']
    URL_LIST      = [
        'customer_id', 'campaign_id', 'adgroup_id', 'status', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'url_display_name', 'ctr']
    CRITERIA_LIST = [
        'customer_id', 'campaign_id', 'adgroup_id', 'keyword_placement', 'keyword_id', 'status', 'cpm_bid', 'cpc_bid', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr',]
    AUDIENCE_LIST = [
        'customer_id', 'campaign_id', 'adgroup_id', 'audience', 'criterion_id', 'status', 'cpm_bid', 'cpc_bid', 'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr']
    AGE_RANGE_LIST = [
        'customer_id', 'campaign_id', 'adgroup_id', 'age_range', 'criterion_id', 'status', 'cpm_bid', 'cpc_bid', 'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr']
    BIDDABLE_LIST = [
        'customer_id', 'campaign_id', 'adgroup_id', 'keyword', 'keyword_id', 'status', 'cpm_bid', 'cpc_bid', 'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr']
    KEYWORDS_LIST = [
        'customer_id', 'campaign_id', 'adgroup_id', 'status', 'keyword', 'keyword_id', 'position', 'serving_status', 'first_page_cpc', 'cpm_bid', 'cpc_bid', 'bidding_type', 'cost_per_target', 'spend', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr'
    ]
    INDEX = {
        'ADGROUP': ADGROUP_LIST,
        'URL': URL_LIST,
        'CRITERIA': CRITERIA_LIST,
        'AUDIENCE': AUDIENCE_LIST,
        'AGE_RANGE': AGE_RANGE_LIST,
        'DISPLAY_KEYWORD': BIDDABLE_LIST,
        'KEYWORDS': KEYWORDS_LIST,
    }

# In[1]:


class Field:
    target_type = 'target_type'
    target = 'target'
    cost_per_target = 'cost_per_target'
    charge_type = 'charge_type'
    start_time = 'start_time'
    stop_time = 'stop_time'
    ai_start_date = 'ai_start_date'
    ai_stop_date = 'ai_stop_date'
    period = 'period'
    daily_budget = 'daily_budget'
    bid_amount = 'bid_amount'
    account_id = 'account_id'
    actions = 'actions'
    adgroup_id = 'adgroup_id'
    campaign_id = 'campaign_id'
    clicks = 'clicks'
    
class DatePreset:
    today = 'TODAY'
    yesterday = 'YESTERDAY'
    lifetime = 'ALL_TIME'
    last_14_days = 'LAST_14_DAYS'

class Campaign(object):
    def __init__(self, customer_id, campaign_id, destination_type):
        self.customer_id = customer_id
        self.campaign_id = campaign_id
        self.destination_type = destination_type
        self.client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
        self.client.SetClientCustomerId(self.customer_id)
        self.report_downloader = self.client.GetReportDownloader(version='v201809')
    
    def get_adgroup_id_list(self):
        ad_group_service = self.client.GetService('AdGroupService', version='v201809')
        # Construct selector and get all ad groups.
        selector = {
            'fields': ['Id', 'Name', 'Status'],
            'predicates': [
                {
                    'field': 'CampaignId',
                    'operator': 'EQUALS',
                    'values': [self.campaign_id]
                },
                {
                    'field': 'Status',
                    'operator': 'EQUALS',
                    'values': ['ENABLED']
                }
            ]
        }
        self.adgroup_id_list = list()
        page = ad_group_service.get(selector)
        if 'entries' in page:
            for ad_group in page['entries']:
                self.adgroup_id_list.append( ad_group['id'] )
        return self.adgroup_id_list
    
    def get_campaign_insights(self, client, date_preset=None):
        if not date_preset:
            date_preset = 'ALL_TIME'
        # Create report definition.
        report = {
            'reportName': 'CAMPAIGN_PERFORMANCE_REPORT',
    #         'dateRangeType': 'CUSTOM_DATE',
            'dateRangeType': date_preset,
            'reportType': 'CAMPAIGN_PERFORMANCE_REPORT',
            'downloadFormat': 'CSV',
            'selector': {
                'fields': CAMPAIGN_FIELDS,
    #             'dateRange': {'min': '20190301','max': '20190401'},
                'predicates': [{
                    'field': 'CampaignId',
                    'operator': 'EQUALS',
                    'values':[self.campaign_id]
                }]
            }
        }
        csv = self.report_downloader.DownloadReportAsString(  
            report, skip_report_header=True, skip_column_header=True,   
            skip_report_summary=True, include_zero_impressions=True,client_customer_id=self.customer_id)
        csv_list = csv.split('\n')[:-1]
        df = pd.DataFrame()
        for lil_csv in csv_list:
            df_temp = pd.DataFrame(
                data=np.array(lil_csv.split(',')).reshape(1,len(CAMPAIGN_FIELDS)),
                columns=CAMPAIGN_FIELDS
            )
            df = df.append(df_temp)
            df['Ctr'] = df.Ctr.str.split('%', expand = True)[0]

        df[df.columns.difference(['CampaignStatus', 'AdvertisingChannelType', 'BiddingStrategyType', 'StartDate', 'EndDate'])] = df[df.columns.difference(
            ['CampaignStatus', 'AdvertisingChannelType', 'BiddingStrategyType', 'StartDate', 'EndDate'])].apply(pd.to_numeric, errors='coerce')
        df[df.columns.difference(['ExternalCustomerId', 'CampaignId', 'AdvertisingChannelType', 'CampaignStatus', 'BiddingStrategyType', 'StartDate', 'EndDate', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'Ctr'])] = df[df.columns.difference(
            ['ExternalCustomerId', 'CampaignId', 'CampaignStatus', 'AdvertisingChannelType', 'BiddingStrategyType', 'StartDate', 'EndDate', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'Ctr'])].div(1000000)
        df[['StartDate','EndDate']] = df[['StartDate','EndDate']].apply( pd.to_datetime, errors='coerce' )
        df.rename( columns=dict( zip(df.columns, DB_CAMPAIGN_COLUMN_NAME_LIST) ), inplace=True )
        self.insights_dict = df.to_dict(orient='records')[0]
        return self.insights_dict
    
    def update_status(client=client, status='PAUSED'):
        # Initialize appropriate service.
        client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
        client.SetClientCustomerId(customer_id)
        campaign_service = client.GetService('CampaignService', version='v201809')

        # Construct operations and update an ad group.
        operations = [{
            'operator': 'SET',
            'operand': {
                'id': self.campaign_id,
                'status': status
            }
        }]

        campaigns = campaign_service.mutate(operations)
        return campaigns
    
    def get_performance_insights(self, client=None,
                                 campaign_id=None,
                                 date_preset=None, performance_type='ADGROUP'):
        report_downloader = self.client.GetReportDownloader(version='v201809')
        if not date_preset:
            date_preset = 'ALL_TIME'
        # Create report definition.
        if self.campaign_id:
            operand = {
                'field': 'CampaignId',
                'operator': 'EQUALS',
                'values': [self.campaign_id]
            }
            fields = ReportField.INDEX[performance_type]
            columns = ReportColumn.INDEX[performance_type]
        else:
            print('get_performance_insights: Missing arguments campaign_id or adgroup_id.')
            operand = None
            return
        report = {
            'downloadFormat': 'CSV',
            'reportName': performance_type+'_PERFORMANCE_REPORT',
            'reportType': performance_type+'_PERFORMANCE_REPORT',
            'dateRangeType': date_preset,
            'selector': {
                'fields': fields,
                'predicates': [operand]
            }
        }
        # Print out the report as a string
        with open(performance_type+'.csv', 'wb') as output_file:
            report_downloader.DownloadReport(
                report, output=output_file, skip_report_header=True, skip_column_header=False,
                skip_report_summary=True, include_zero_impressions=False)
        with open(performance_type+'.csv')as csv_file:
            df = pd.read_csv(csv_file, sep=",", quotechar='"')
            try:
                df['CTR'] = df.CTR.str.split('%', expand = True)[0]
            except KeyError as e:
                print('[gdn_datacollector.Campaign.get_performance_insights]: ', performance_type, e)
                pass
            df[df.columns.difference( ReportField.NON_NUMERIC_LIST )] = df[df.columns.difference( ReportField.NON_NUMERIC_LIST )].apply(pd.to_numeric, errors='coerce')
            df[df.columns.intersection( ReportField.NUMERIC_LIST )] = df[df.columns.intersection( ReportField.NUMERIC_LIST )].div(1000000)
            df.columns = columns
            df.sort_values(by=['impressions'], ascending=False).reset_index(drop=True)
            gdn_db.into_table( df, performance_type.lower()+'_insights' )
            return df

# In[10]:


class AdGroup(Campaign):
    def __init__(self, customer_id, campaign_id, destination_type, adgroup_id):
        super().__init__(customer_id, campaign_id, destination_type)
        self.customer_id = customer_id
        self.campaign_id = campaign_id
        self.adgroup_id = adgroup_id
        self.client = client
        self.client.SetClientCustomerId(self.customer_id)
        self.report_downloader = self.client.GetReportDownloader(version='v201809')
        
    def get_adgroup_insights(self, client, date_preset='ALL_TIME', by_device=False, by_hour=False):
        self.report_metrics = [
            'ExternalCustomerId','CampaignId', 'AdGroupType', 'AdGroupId', 'AdGroupStatus', 'CpmBid','CpvBid', 'CpcBid',
            'TargetCpa', 'BiddingStrategyType','Cost', 'AverageCost','Impressions', 'Clicks','Conversions', 'AllConversions', 'AverageCpc',
            'CostPerConversion', 'CostPerAllConversion', 'Ctr',]
        self.db_column_name_list = [
            'customer_id', 'campaign_id', 'channel_type', 'adgroup_id', 'status', 'cpm_bid', 'cpv_bid', 'cpc_bid', 'cpa_bid',
            'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions',
            'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr' ]
        if by_device:
            self.report_metrics.append('Device')
            self.db_column_name_list.append('device')
        if by_hour:
            self.report_metrics.append('HourOfDay')
            self.db_column_name_list.append('hour_of_day')
            self.report_metrics.remove('AllConversions')
            self.db_column_name_list.remove('all_conversions')
            self.report_metrics.remove('CostPerAllConversion')
            self.db_column_name_list.remove('cost_per_all_conversion')
            
        # Create report definition.
        if self.campaign_id is not None and self.adgroup_id is None:
            operand = {
                        'field': 'CampaignId',
                        'operator': 'EQUALS',
                        'values':[self.campaign_id]
            }
        elif self.adgroup_id is not None:
            operand = {
                        'field': 'AdGroupId',
                        'operator': 'EQUALS',
                        'values':[self.adgroup_id]
            }
        else:
            print('get_adgroup_insights: Missing arguments campaign_id or adgroup_id.' )
            operand = None
            return
        report = {
            'reportName': 'ADGROUP_PERFORMANCE_REPORT',
    #         'dateRangeType': 'CUSTOM_DATE',
            'dateRangeType': date_preset,
            'reportType': 'ADGROUP_PERFORMANCE_REPORT',
            'downloadFormat': 'CSV',
            'selector': {
                'fields': self.report_metrics,
    #             'dateRange': {'min': '20190301','max': '20190401'},
                'predicates': [
                    operand
                ]
            }
        }
        # Print out the report as a string
        csv =self.report_downloader.DownloadReportAsString(  
            report, skip_report_header=True, skip_column_header=True,   
            skip_report_summary=True, include_zero_impressions=True,client_customer_id=self.customer_id)
        csv_list = csv.split('\n')[:-1]
        df = pd.DataFrame()
        for lil_csv in csv_list:
            df_temp = pd.DataFrame(
                data=np.array(lil_csv.split(',')).reshape(1,len(self.report_metrics)),
                columns=self.report_metrics
            )
            df = df.append(df_temp)
            df['Ctr'] = df.Ctr.str.split('%', expand = True)[0]
        df[df.columns.difference(['AdGroupType', 'AdGroupStatus', 'BiddingStrategyType', 'Device'])] = df[df.columns.difference(
            ['AdGroupType', 'AdGroupStatus', 'BiddingStrategyType', 'Device'])].apply(pd.to_numeric, errors='coerce')
        df[df.columns.difference( ['HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'Conversions', 'Ctr'] )] = df[df.columns.difference(
            ['HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'Conversions', 'Ctr'])].div(1000000)
        df.rename( columns=dict( zip(df.columns, self.db_column_name_list) ), inplace=True )
#         return df
        self.insights_dict = df.reset_index(drop=True).to_dict(orient='records')
        return self.insights_dict

    def update_status(self, client=client, status='PAUSED'):
        # Initialize appropriate service.
        client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
        client.SetClientCustomerId(self.customer_id)
        ad_group_service = client.GetService('AdGroupService', version='v201809')
        # Construct operations and update an ad group.
        operations = [{
            'operator': 'SET',
            'operand': {
                'id': self.adgroup_id,
                'status': status
            }
        }]
        adgroups = ad_group_service.mutate(operations)
        return adgroups

# In[6]:


def update_adgroup_bid(customer_id, ad_group_id, bid_micro_amount=None, client=client):
    # Initialize appropriate service.
    client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
    client.SetClientCustomerId(customer_id)
    ad_group_service = client.GetService('AdGroupService', version='v201809')
    
    # Construct operations and update an ad group.
    operations = [{
        'operator': 'SET',
        'operand': {
            'id': ad_group_id,
        }
    }]
  
    if bid_micro_amount:
        bid_micro_amount = int(bid_micro_amount * 1000000)
        operations[0]['operand']['biddingStrategyConfiguration'] = {
            'bids': [{
                'xsi_type': 'CpcBid',
                'bid': {
                    'microAmount': bid_micro_amount,
                }
            }]
        }
  
    ad_groups = ad_group_service.mutate(operations)
    return ad_groups


# In[7]:


def data_collect(customer_id, campaign_id, destination, destination_type, ai_start_date, ai_stop_date):
    camp = Campaign(customer_id, campaign_id, destination_type)
    ###
    campaign_lifetime_insights = camp.get_campaign_insights( client, date_preset=DatePreset.lifetime )
#     campaign_today_insights = camp.get_campaign_insights( client, date_preset=DatePreset.today )
    ###
    addition_column_list = [ 'period', 'period_left', 'target', 'target_left', 'daily_target', 'destination', 'destination_type' ]
    period = ( ai_stop_date - ai_start_date ).days + 1
    period_left = ( ai_stop_date-datetime.datetime.now().date() ).days + 1
    if period_left == 0:
        period_left = 1
    target = campaign_lifetime_insights[ CAMPAIGN_OBJECTIVE_FIELD[ destination_type ] ]
    target_left = int(destination) - campaign_lifetime_insights[ CAMPAIGN_OBJECTIVE_FIELD[ destination_type ] ]
#     try:
    daily_target = target_left / period_left
        
    addition_value_list = [period, period_left, target, target_left, daily_target, destination, destination_type]
    addition_dict = dict( zip(addition_column_list, addition_value_list))
    campaign_dict = {
        **campaign_lifetime_insights,
        **addition_dict,
    }
    df_campaign = pd.DataFrame(campaign_dict, index=[0])
    gdn_db.update_table(df_campaign, table="campaign_target")
    adgroup_id_list = camp.get_adgroup_id_list()

    for adgroup_id in adgroup_id_list:
        adgroup = AdGroup(camp.customer_id,camp.campaign_id,camp.destination_type, adgroup_id)
        adgroup_today_insights = adgroup.get_adgroup_insights(client, date_preset=DatePreset.today)
        df_adgroup = pd.DataFrame(adgroup_today_insights)
        gdn_db.into_table(df_adgroup, table="adgroup_insights")
        bidding_type = adgroup_today_insights[0]['bidding_type']
        bid_amount_column = BIDDING_INDEX[ bidding_type ]
        df_adgroup['bid_amount'] = df_adgroup[bid_amount_column]
        df_adgroup['bid_amount'] = math.ceil(reverse_bid_amount(df_adgroup[bid_amount_column]))
        gdn_db.check_initial_bid(adgroup_id, df_adgroup[[Field.campaign_id, Field.adgroup_id, Field.bid_amount]])

    return


# In[8]:


def main():
    start_time = datetime.datetime.now()
    df_camp = gdn_db.get_campaign_is_running()
    print(df_camp['campaign_id'].unique())
    for campaign_id in df_camp['campaign_id'].unique():
        print('[campaign_id]: ', campaign_id)
        customer_id = df_camp['customer_id'][df_camp.campaign_id==campaign_id].iloc[0]
        destination = df_camp['destination'][df_camp.campaign_id==campaign_id].iloc[0]
        destination_type = df_camp['destination_type'][df_camp.campaign_id==campaign_id].iloc[0]
        ai_start_date = df_camp['ai_start_date'][df_camp.campaign_id==campaign_id].iloc[0]
        ai_stop_date = df_camp['ai_stop_date'][df_camp.campaign_id==campaign_id].iloc[0]
        data_collect( customer_id, int(campaign_id), destination, destination_type, ai_start_date, ai_stop_date )


        
    print(datetime.datetime.now()-start_time)


# In[2]:


if __name__=='__main__':
    main()
#     df_campaign = data_collect(camp.customer_id, camp.campaign_id, 10000, camp.destination_type)


# In[3]:


#!jupyter nbconvert --to script gdn_datacollector.ipynb


# In[4]:


# CUSTOMER_ID = 2042877296
# CAMPAIGN_ID = 1984860137
# client.SetClientCustomerId(CUSTOMER_ID)
# report_downloader = client.GetReportDownloader(version='v201809')

# FIELDS = [
#     'ContentImpressionShare','HourOfDay'
# ]

# def get_campaign_insights(campaign_id=None, date_preset=None):
#     if not date_preset:
#         date_preset = 'ALL_TIME'
#     # Create report definition.
#     report = {
#         'reportName': 'CAMPAIGN_PERFORMANCE_REPORT',
# #         'dateRangeType': 'CUSTOM_DATE',
#         'dateRangeType': date_preset,
#         'reportType': 'CAMPAIGN_PERFORMANCE_REPORT',
#         'downloadFormat': 'CSV',
#         'selector': {
#             'fields': FIELDS,
# #             'dateRange': {'min': '20190301','max': '20190401'},
#             'predicates': [{
#                 'field': 'CampaignId',
#                 'operator': 'EQUALS',
#                 'values':[campaign_id]
#             }]
#         }
#     }
#     csv = report_downloader.DownloadReportAsString(  
#         report, skip_report_header=True, skip_column_header=True,   
#         skip_report_summary=True, include_zero_impressions=False,client_customer_id=CUSTOMER_ID)
#     csv_list = csv.split('\n')[:-1]
#     df = pd.DataFrame()
#     for lil_csv in csv_list:
#         df_temp = pd.DataFrame(
#             data=np.array(lil_csv.split(',')).reshape(1,len(FIELDS)),
#             columns=FIELDS
#         )
#         df = df.append(df_temp)
#     return df


# In[ ]:




