#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import uuid
from googleads import adwords
import sys
import pandas as pd
import numpy as np
import math
import datetime
from bid_operator import reverse_bid_amount
import adgeek_permission as permission
import database_controller
import google_adwords_controller as controller


# In[ ]:


CAMPAIGN_OBJECTIVE_FIELD = {
    'LINK_CLICKS': 'clicks',
    'CONVERSIONS':'conversions',
    'cpc': 'clicks',
    'cpa':'conversions',
}

CAMPAIGN_FIELDS = [
    'ExternalCustomerId', 'CampaignId', 'AdvertisingChannelType', 'CampaignStatus', 'BiddingStrategyType', 'Amount', 'Cost', 'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion',
    'CostPerAllConversion', 'Ctr', 'ViewThroughConversions'
]
DB_CAMPAIGN_COLUMN_NAME_LIST = [
    'customer_id', 'campaign_id', 'channel_type', 'status', 'bidding_type', 'daily_budget', 'spend',
    'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion',
    'cost_per_all_conversion', 'ctr', 'view_conversions'
]
BIDDING_INDEX = {
    'cpc': 'cpc_bid',
    'Target CPA': 'cpa_bid',
}


# In[ ]:


class ReportField:
    # URL
    URL_LIST = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'AdGroupStatus', 'Cost', 'AverageCost', 'Impressions', 'Clicks',
        'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'DisplayName', 'Ctr', 'ViewThroughConversions']
    # CRITERIA,
    CRITERIA_LIST = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'Criteria', 'Id', 'AdGroupStatus', 'CpmBid', 'CpcBid', 'Cost',
        'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'Ctr', 'ViewThroughConversions']
    # AUDIENCE, AGE_RANGE, DISPLAY_KEYWORD
    BIDDABLE_LIST = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'Criteria', 'Id', 'AdGroupStatus', 'CpmBid', 'CpcBid', 'BiddingStrategyType', 
        'Cost', 'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'Ctr', 'ViewThroughConversions']
    # DISPLAY_TOPICS
    TOPICS_LIST = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'Criteria', 'Id', 'VerticalId', 'AdGroupStatus', 'CpmBid', 'CpcBid', 'BiddingStrategyType', 
        'Cost', 'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'Ctr', 'ViewThroughConversions']    # ADGROUP
    ADGROUP_LIST = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupType', 'AdGroupId', 'AdGroupStatus', 'CpmBid', 'CpvBid', 'CpcBid', 'TargetCpa',
        'BiddingStrategyType', 'Cost', 'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'Ctr', 'ViewThroughConversions']
    # KEYWORDS
    KEYWORDS_LIST = [
        'ExternalCustomerId','CampaignId', 'AdGroupId', 'AdGroupStatus', 'Criteria', 'Id','AveragePosition', 'SystemServingStatus', 'FirstPageCpc',
        'CpmBid', 'CpcBid', 'BiddingStrategyType', 'AverageCost','Cost','Impressions', 'Clicks','Conversions', 'AllConversions', 'AverageCpc',
        'CostPerConversion', 'CostPerAllConversion', 'Ctr', 'ViewThroughConversions']
    
    NON_NUMERIC_LIST = [
        'Criterion serving status', 'Ad group type', 'Ad group state', 'Bid Strategy Type', 'Keyword', 'Age Range', 'Audience',
        'Keyword / Placement', 'Criteria Display Name', 'Topic']
    NUMERIC_LIST = [
        'First page CPC', 'Max. CPM', 'Max. CPV', 'Max. CPC', 'Default max. CPC', 'Target CPA', 'Cost', 'Avg. Cost', 'Avg. CPC',
        'Cost / conv.', 'Cost / all conv.', 'ViewThroughConversions']
    INDEX = {
        'ADGROUP': ADGROUP_LIST,
        'URL': URL_LIST,
        'CRITERIA': CRITERIA_LIST,
        'AUDIENCE': BIDDABLE_LIST,
        'AGE_RANGE': BIDDABLE_LIST,
        'DISPLAY_KEYWORD': BIDDABLE_LIST,
        'DISPLAY_TOPICS': TOPICS_LIST,
        'KEYWORDS': KEYWORDS_LIST,
    }


# In[ ]:


class ReportColumn:
    ADGROUP_LIST  = [
        'customer_id', 'campaign_id', 'channel_type', 'adgroup_id', 'status', 'cpm_bid', 'cpv_bid', 'cpc_bid', 'cpa_bid', 'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 'view_conversions']
    URL_LIST      = [
        'customer_id', 'campaign_id', 'adgroup_id', 'status', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'url_display_name', 'ctr', 'view_conversions']
    CRITERIA_LIST = [
        'customer_id', 'campaign_id', 'adgroup_id', 'keyword_placement', 'keyword_id', 'status', 'cpm_bid', 'cpc_bid', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 'view_conversions']
    AUDIENCE_LIST = [
        'customer_id', 'campaign_id', 'adgroup_id', 'audience', 'criterion_id', 'status', 'cpm_bid', 'cpc_bid', 'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 'view_conversions']
    AGE_RANGE_LIST = [
        'customer_id', 'campaign_id', 'adgroup_id', 'age_range', 'criterion_id', 'status', 'cpm_bid', 'cpc_bid', 'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 'view_conversions']
    TOPICS_LIST = [
        'customer_id', 'campaign_id', 'adgroup_id', 'topics', 'criterion_id', 'vertical_id', 'status', 'cpm_bid', 'cpc_bid', 'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 'view_conversions']
    BIDDABLE_LIST = [
        'customer_id', 'campaign_id', 'adgroup_id', 'keyword', 'keyword_id', 'status', 'cpm_bid', 'cpc_bid', 'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 'view_conversions']
    KEYWORDS_LIST = [
        'customer_id', 'campaign_id', 'adgroup_id', 'status', 'keyword', 'keyword_id', 'position', 'serving_status', 'first_page_cpc', 'cpm_bid', 'cpc_bid', 'bidding_type', 'cost_per_target', 'spend', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 'view_conversions'
    ]
    INDEX = {
        'ADGROUP': ADGROUP_LIST,
        'URL': URL_LIST,
        'CRITERIA': CRITERIA_LIST,
        'AUDIENCE': AUDIENCE_LIST,
        'AGE_RANGE': AGE_RANGE_LIST,
        'DISPLAY_KEYWORD': BIDDABLE_LIST,
        'DISPLAY_TOPICS': TOPICS_LIST,
        'KEYWORDS': KEYWORDS_LIST,
    }


# In[ ]:


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


# In[ ]:


class DatePreset:
    today = 'TODAY'
    yesterday = 'YESTERDAY'
    lifetime = 'ALL_TIME'
    last_14_days = 'LAST_14_DAYS'


# In[ ]:


class Campaign(object):
    def __init__(self, customer_id, campaign_id, destination_type=None, database_gdn=None):
        self.customer_id = customer_id
        self.campaign_id = campaign_id
        self.database_gdn = database_gdn
        self.destination_type = destination_type
        self.client = permission.init_google_api(self.customer_id)
        self.report_downloader = self.client.GetReportDownloader(version='v201809')
        if self.database_gdn is None:
            self.database_gdn = database_controller.GDN( database_controller.Database() )
        brief_dict = self.database_gdn.get_brief( self.campaign_id )
        self.ai_start_date = brief_dict['ai_start_date'].strftime("%Y%m%d")
        self.ai_stop_date = brief_dict['ai_stop_date'].strftime("%Y%m%d")
        self.ai_spend_cap = brief_dict['ai_spend_cap']
        self.destination_type = brief_dict['destination_type']
    
    def get_campaign_insights(self, date_preset=None):
        # Create report definition.
        date_range = { 'min': None, 'max': None }
        selector = {
            'fields': CAMPAIGN_FIELDS,
            'predicates': [{
                'field': 'CampaignId',
                'operator': 'EQUALS',
                'values':[self.campaign_id]
            }]
        }
        report = {
            'reportName': 'CAMPAIGN_PERFORMANCE_REPORT',
            'reportType': 'CAMPAIGN_PERFORMANCE_REPORT',
            'downloadFormat': 'CSV',
            'selector': selector
        }

        if not date_preset or date_preset == DatePreset.lifetime:
            date_range['min'], date_range['max'] = self.ai_start_date, self.ai_stop_date
            selector['dateRange'] = date_range
            report['dateRangeType'] = 'CUSTOM_DATE'
            report['selector'] = selector
        else:
            report['dateRangeType'] = date_preset

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
        df[df.columns.difference(['ExternalCustomerId', 'CampaignId', 'AdvertisingChannelType', 'CampaignStatus', 'BiddingStrategyType', 'StartDate', 'EndDate', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'Ctr', 'ViewThroughConversions'])] = df[df.columns.difference(
            ['ExternalCustomerId', 'CampaignId', 'CampaignStatus', 'AdvertisingChannelType', 'BiddingStrategyType', 'StartDate', 'EndDate', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'Ctr', 'ViewThroughConversions'])].div(1000000)
#         df[['StartDate','EndDate']] = df[['StartDate','EndDate']].apply( pd.to_datetime, errors='coerce' )
        df.rename( columns=dict( zip(df.columns, DB_CAMPAIGN_COLUMN_NAME_LIST) ), inplace=True )
        self.insights_dict = df.to_dict(orient='records')[0]
        return self.insights_dict
    
    def get_performance_insights(self, client=None,
                                 campaign_id=None,
                                 date_preset=None, performance_type='ADGROUP'):
        report_downloader = self.client.GetReportDownloader(version='v201809')
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
        date_range = { 'min': None, 'max': None }
        selector = {
            'fields': fields,
            'predicates': [operand]
        }
        report = {
            'downloadFormat': 'CSV',
            'reportName': performance_type+'_PERFORMANCE_REPORT',
            'reportType': performance_type+'_PERFORMANCE_REPORT',
            'selector': selector
        }
        if not date_preset or date_preset == DatePreset.lifetime:
            date_range['min'], date_range['max'] = self.ai_start_date, self.ai_stop_date
            selector['dateRange'] = date_range
            report['dateRangeType'] = 'CUSTOM_DATE'
            report['selector'] = selector
        else:
            report['dateRangeType'] = date_preset

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
            df = df.where((pd.notnull(df)), None)
            value_dict_list = df.to_dict('records')
            for val in value_dict_list:
                self.database_gdn.upsert( performance_type.lower()+'_insights', val )
            return df


# In[ ]:


class AdGroup(Campaign):
    def __init__(self, customer_id, campaign_id, adgroup_id, destination_type=None, database_gdn=None):
        super().__init__(customer_id, campaign_id, destination_type, database_gdn)
        self.customer_id = customer_id
        self.campaign_id = campaign_id
        self.database_gdn = database_gdn
        self.adgroup_id = adgroup_id
        self.client = permission.init_google_api(self.customer_id)
        self.report_downloader = self.client.GetReportDownloader(version='v201809')
        if self.database_gdn is None:
            self.database_gdn = database_controller.GDN( database_controller.Database() )
        brief_dict = self.database_gdn.get_brief( self.campaign_id )
        self.ai_start_date = brief_dict['ai_start_date'].strftime("%Y%m%d")
        self.ai_stop_date = brief_dict['ai_stop_date'].strftime("%Y%m%d")
        self.ai_spend_cap = brief_dict['ai_spend_cap']
        self.destination_type = brief_dict['destination_type']
        
        
    def get_adgroup_insights(self, date_preset=None, by_device=False, by_hour=False):
        self.report_metrics = [
            'ExternalCustomerId','CampaignId', 'AdGroupType', 'AdGroupId', 'AdGroupStatus', 'CpmBid','CpvBid', 'CpcBid',
            'TargetCpa', 'BiddingStrategyType','Cost', 'AverageCost','Impressions', 'Clicks','Conversions', 'AllConversions', 'AverageCpc',
            'CostPerConversion', 'CostPerAllConversion', 'Ctr', 'ViewThroughConversions']
        self.db_column_name_list = [
            'customer_id', 'campaign_id', 'channel_type', 'adgroup_id', 'status', 'cpm_bid', 'cpv_bid', 'cpc_bid', 'cpa_bid',
            'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions',
            'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 'view_conversions' ]
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
        date_range = { 'min': None, 'max': None }
        selector = {
            'fields': self.report_metrics,
            'predicates': [operand]
        }
        report = {
            'reportName': 'ADGROUP_PERFORMANCE_REPORT',
            'dateRangeType': date_preset,
            'reportType': 'ADGROUP_PERFORMANCE_REPORT',
            'downloadFormat': 'CSV',
            'selector': selector
        }
        if not date_preset or date_preset == DatePreset.lifetime:
            date_range['min'], date_range['max'] = self.ai_start_date, self.ai_stop_date
            selector['dateRange'] = date_range
            report['dateRangeType'] = 'CUSTOM_DATE'
            report['selector'] = selector
        else:
            report['dateRangeType'] = date_preset
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
        df[df.columns.difference( ['HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'AllConversions', 'Conversions', 'Ctr'] )] = df[df.columns.difference(
            ['HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'AllConversions', 'Conversions', 'Ctr'])].div(1000000)
        df.rename( columns=dict( zip(df.columns, self.db_column_name_list) ), inplace=True )
#         return df
        self.insights_dict = df.reset_index(drop=True).to_dict(orient='records')
        return self.insights_dict


# In[ ]:


def data_collect(database_gdn, campaign):
    
    customer_id = campaign.get("customer_id")
    campaign_id = campaign.get("campaign_id")
    destination = campaign.get("destination")
    destination_type = campaign.get("destination_type")
    ai_start_date = campaign.get("ai_start_date")
    ai_stop_date = campaign.get("ai_stop_date")
    
    service_container = controller.AdGroupServiceContainer(customer_id=customer_id)
    camp = Campaign(customer_id, campaign_id, destination_type, database_gdn)
    controller_campaign = controller.Campaign(service_container=service_container, campaign_id=campaign_id)
    ###
    campaign_lifetime_insights = camp.get_campaign_insights( date_preset=DatePreset.lifetime )
#     campaign_today_insights = camp.get_campaign_insights( client, date_preset=DatePreset.today )
    ###
    addition_column_list = [ 'period', 'period_left', 'target', 'target_left', 'daily_target', 'destination', 'destination_type' ]
    period = ( ai_stop_date - ai_start_date ).days + 1
    period_left = ( ai_stop_date-datetime.datetime.now().date() ).days + 1
    if period_left == 0:
        period_left = 1
    target = campaign_lifetime_insights[ CAMPAIGN_OBJECTIVE_FIELD[ destination_type ] ]
    target_left = int(destination) - campaign_lifetime_insights[ CAMPAIGN_OBJECTIVE_FIELD[ destination_type ] ]
    daily_target = target_left / period_left
        
    addition_value_list = [period, period_left, target, target_left, daily_target, destination, destination_type]
    addition_dict = dict( zip(addition_column_list, addition_value_list) )
    campaign_dict = {
        **campaign_lifetime_insights,
        **addition_dict,
    }
    database_gdn.upsert("campaign_target", campaign_dict)
    ad_group_list = controller_campaign.get_ad_groups()
    for ad_group in ad_group_list:
        adgroup_id = ad_group.ad_group_id
        adgroup = AdGroup(camp.customer_id,camp.campaign_id, adgroup_id,camp.destination_type)
        adgroup_today_insights = adgroup.get_adgroup_insights(date_preset=DatePreset.today)
        if adgroup_today_insights:
            df_adgroup = pd.DataFrame(adgroup_today_insights)
            database_gdn.insert("adgroup_insights", df_adgroup.fillna(0).to_dict('records')[0])
            bidding_type = adgroup_today_insights[0]['bidding_type']
            bid_amount_column = BIDDING_INDEX[ bidding_type ]
            adgroup_today_insights[0]['bid_amount'] = adgroup_today_insights[0][bid_amount_column]
            adgroup_today_insights[0]['bid_amount'] = math.ceil(reverse_bid_amount(adgroup_today_insights[0][bid_amount_column]))
            database_gdn.insert_ignore("adgroup_initial_bid", { key : adgroup_today_insights[0][key] for key in [ Field.campaign_id, Field.adgroup_id, Field.bid_amount ] })
    database_gdn.dispose()


# In[ ]:


def main():
    start_time = datetime.datetime.now()
    db = database_controller.Database()
    database_gdn = database_controller.GDN(db)
    campaign_running_list = database_gdn.get_running_campaign().to_dict('records')
    print([campaign['campaign_id'] for campaign in campaign_running_list])
    for campaign in campaign_running_list:
        print('[campaign_id]: ', campaign.get('campaign_id'))
        
        data_collect( database_gdn, campaign )

    print(datetime.datetime.now()-start_time)


# In[ ]:


if __name__=='__main__':
    main()
#     df_campaign = data_collect(camp.customer_id, camp.campaign_id, 10000, camp.destination_type)


# In[ ]:


# !jupyter nbconvert --to script gdn_datacollector.ipynb


# In[ ]:


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




