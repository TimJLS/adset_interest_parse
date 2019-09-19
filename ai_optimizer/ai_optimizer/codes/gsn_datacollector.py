#!/usr/bin/env python
# coding: utf-8

# In[1]:


from googleads import adwords
import pandas as pd
import numpy as np
import math
import datetime
import gsn_db
import google_adwords_controller as controller
import gdn_datacollector as datacollector
import bid_operator
import adgeek_permission as permission

CAMPAIGN_FIELDS = [
    'ExternalCustomerId','CampaignId', 'TopImpressionPercentage','AdvertisingChannelType', 'CampaignStatus', 'BiddingStrategyType',
    'Amount','StartDate','EndDate','Cost', 'AverageCost','Impressions', 'Clicks','Conversions', 'AllConversions', 'AverageCpc',
    'CostPerConversion', 'CostPerAllConversion', 'Ctr'
]
DB_CAMPAIGN_COLUMN_NAME_LIST = [
    'customer_id', 'campaign_id', 'top_impression_percentage', 'channel_type', 'status', 'bidding_type', 'daily_budget', 'start_time', 'stop_time',
    'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr',
]
NON_NUMERIC_LIST = ['Criterion serving status',
 'Ad group type',
 'Ad group state',
 'Bid Strategy Type',
 'Keyword',
 'Age Range',
 'Audience',
 'Keyword / Placement',
 'Criteria Display Name', 'Advertising Channel', 'Campaign state', 'Start date', 'End date']
NUMERIC_LIST = ['First page CPC',
 'Max. CPM',
 'Max. CPV',
 'Max. CPC',
 'Default max. CPC',
 'Target CPA',
 'Cost',
 'Avg. Cost',
 'Avg. CPC',
 'Cost / conv.',
 'Cost / all conv.', 'Budget']
class DatePreset:
    today = 'TODAY'
    yesterday = 'YESTERDAY'
    lifetime = 'ALL_TIME'
    last_14_days = 'LAST_14_DAYS'

# In[3]:


class Campaign(object):
    report_metrics = [
        'ExternalCustomerId','CampaignId', 'AdGroupId', 'AdGroupStatus', 'Criteria', 'Id', 'CpmBid', 'CpcBidSource', 'CpcBid',
        'BiddingStrategyType', 'FirstPageCpc','Cost', 'AverageCost','Impressions', 'Clicks','Conversions', 'AverageCpc',
        'CostPerConversion', 'Ctr','TopImpressionPercentage', 'SystemServingStatus']
    operand = {
        'field': 'CampaignId',
        'operator': 'EQUALS',
        'values': None
    }
    date_range = { 'min': None, 'max': None }
    selector = {
        'fields': None,
        'predicates': [operand]
    }
    report = {
        'reportName': None,
        'dateRangeType': 'ALL_TIME',
        'reportType': None,
        'downloadFormat': 'CSV',
        'selector': selector
    }
    def __init__(self, customer_id, campaign_id):
        self.customer_id = customer_id
        self.client = permission.init_google_api(self.customer_id)
        self.campaign_id = campaign_id
        self.report_downloader = self.client.GetReportDownloader(version='v201809')
        self.brief_dict = gsn_db.get_campaign_ai_brief(campaign_id=self.campaign_id)
        self.ai_start_date = self.brief_dict['ai_start_date']
        self.ai_stop_date = self.brief_dict['ai_stop_date']
        self.ai_spend_cap = self.brief_dict['ai_spend_cap']
        self.destination_type = self.brief_dict['destination_type']
        self.destination = self.brief_dict['destination']
        
        self.ad_group_criterion_service = self.client.GetService('AdGroupCriterionService', version='v201809')

    def get_performance_insights(self, date_preset=None, performance_type='KEYWORDS'):
        fields = datacollector.ReportField.INDEX[performance_type] if performance_type!='CAMPAIGN' else CAMPAIGN_FIELDS
        columns = datacollector.ReportColumn.INDEX[performance_type] if performance_type!='CAMPAIGN' else DB_CAMPAIGN_COLUMN_NAME_LIST
        
        self.operand['values']=[self.campaign_id]
        self.report['reportName']=performance_type+'_PERFORMANCE_REPORT'
        self.report['reportType']=performance_type+'_PERFORMANCE_REPORT'
        self.selector['fields']=fields
#         self.report['selector']['fields']=fields
      
        if date_preset is None or date_preset == datacollector.DatePreset.lifetime:
            self.date_range['min'], self.date_range['max'] = self.ai_start_date, self.ai_stop_date
            self.selector['dateRange'] = self.date_range
            self.report['dateRangeType'] = 'CUSTOM_DATE'
            self.report['selector'] = self.selector
        else:
            self.report['dateRangeType'] = date_preset
        with open(performance_type+'.csv', 'wb') as output_file:
            self.report_downloader.DownloadReport(
                self.report, output=output_file, skip_report_header=True, skip_column_header=False,
                skip_report_summary=True, include_zero_impressions=False)
        with open(performance_type+'.csv')as csv_file:
            df = pd.read_csv(csv_file, sep=",", quotechar='"')
            try:
                df['CTR'] = df.CTR.str.split('%', expand = True)[0]
            except KeyError as e:
                print('[gsn_datacollector.Campaign.get_performance_insights]: ', performance_type, e)
                pass
            df[df.columns.difference( NON_NUMERIC_LIST )] = df[df.columns.difference( NON_NUMERIC_LIST )].apply(pd.to_numeric, errors='coerce')
            df[df.columns.intersection( NUMERIC_LIST )] = df[df.columns.intersection( NUMERIC_LIST )].div(1000000)
            df.columns = columns
            df.sort_values(by=['impressions'], ascending=False).reset_index(drop=True)
            if df.empty:
                return df
            if performance_type=='CAMPAIGN':
                gsn_db.update_table(df, table="campaign_target")
            else:
                gsn_db.into_table( df, performance_type.lower()+'_insights' )
            return df
        
    def get_keyword_insights(self, date_preset=None):
        self.report_metrics = [
            'ExternalCustomerId','CampaignId', 'AdGroupId', 'AdGroupStatus', 'Criteria', 'Id','TopImpressionPercentage', 'SystemServingStatus', 'FirstPageCpc',
            'CpmBid', 'CpcBid', 'BiddingStrategyType', 'AverageCost','Cost','Impressions', 'Clicks','Conversions', 'AverageCpc',
            'CostPerConversion', 'Ctr']
        self.db_column_name_list = [
            'customer_id', 'campaign_id', 'adgroup_id', 'status', 'keyword', 'keyword_id', 'top_impression_percentage', 'serving_status', 'first_page_cpc', 'cpm_bid', 'cpc_bid', 'bidding_type', 'cost_per_target', 'spend', 'impressions', 'clicks', 'conversions', 'cost_per_click', 'cost_per_conversion', 'ctr'
        ]
        self.operand = [{
                'field': 'CampaignId',
                'operator': 'IN',
                'values':[self.campaign_id]
            }]
        self.report = {
            'reportName': 'KEYWORDS_PERFORMANCE_REPORT',
    #         'dateRangeType': 'CUSTOM_DATE',
            'dateRangeType': date_preset,
            'reportType': 'KEYWORDS_PERFORMANCE_REPORT',
            'downloadFormat': 'CSV',
            'selector': {
                'fields': self.report_metrics,
    #             'dateRange': {'min': '20190301','max': '20190401'},
                'predicates': self.operand
            }
        }
        # Print out the report as a string
        csv =self.report_downloader.DownloadReportAsString(  
            self.report, skip_report_header=True, skip_column_header=True,   
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
        df[df.columns.difference(['Criteria','SystemServingStatus', 'AdGroupType', 'AdGroupStatus', 'BiddingStrategyType', 'Device'])] = df[df.columns.difference(
            ['Criteria','SystemServingStatus', 'AdGroupType', 'AdGroupStatus', 'BiddingStrategyType', 'Device'])].apply(pd.to_numeric, errors='coerce')
        df[df.columns.difference( ['TopImpressionPercentage','Id','Criteria','SystemServingStatus', 'HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'Conversions', 'Ctr'] )] = df[df.columns.difference(
            ['TopImpressionPercentage','Id','Criteria','SystemServingStatus','HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'Conversions', 'Ctr'])].div(1000000)
        df.rename( columns=dict( zip(df.columns, self.db_column_name_list) ), inplace=True )
        self.insights_dict = df.reset_index(drop=True).to_dict(orient='records')
        return self.insights_dict


# In[4]:


class KeywordGroup(object):
    def __init__(self, customer_id, campaign_id, keyword_id):
        self.customer_id = customer_id
        self.campaign_id = campaign_id
        self.keyword_id = keyword_id
        self.client = permission.init_google_api(self.customer_id)
        self.report_downloader = self.client.GetReportDownloader(version='v201809')
        self.ad_group_criterion_service = self.client.GetService('AdGroupCriterionService', version='v201809')
        brief_dict = gsn_db.get_campaign_ai_brief( self.campaign_id )
        self.ai_start_date = brief_dict['ai_start_date'].strftime("%Y%m%d")
        self.ai_stop_date = brief_dict['ai_stop_date'].strftime("%Y%m%d")
        self.ai_spend_cap = brief_dict['ai_spend_cap']
        self.destination_type = brief_dict['destination_type']
    
    def get_keyword_insights(self, date_preset='ALL_TIME'):
        self.report_metrics = [
            'ExternalCustomerId','CampaignId', 'AdGroupId', 'AdGroupStatus', 'Criteria', 'Id','TopImpressionPercentage', 'SystemServingStatus', 'FirstPageCpc',
            'CpmBid', 'CpcBid', 'BiddingStrategyType', 'AverageCost','Cost','Impressions', 'Clicks','Conversions', 'AverageCpc',
            'CostPerConversion', 'Ctr']
        self.db_column_name_list = [
            'customer_id', 'campaign_id', 'adgroup_id', 'status', 'keyword', 'keyword_id', 'top_impression_percentage', 'serving_status', 'first_page_cpc', 'cpm_bid', 'cpc_bid', 'bidding_type', 'cost_per_target', 'spend', 'impressions', 'clicks', 'conversions', 'cost_per_click', 'cost_per_conversion', 'ctr'
        ]
        self.operand = [
            {
                'field': 'Id',
                'operator': 'IN',
                'values':[self.keyword_id]
            },{
                'field': 'CampaignId',
                'operator': 'IN',
                'values':[self.campaign_id]
            }
        ]
        self.date_range = { 'min': None, 'max': None }
        self.selector = {
            'fields': self.report_metrics,
            'predicates': [self.operand]
        }
        self.report = {
            'reportName': 'KEYWORDS_PERFORMANCE_REPORT',
    #         'dateRangeType': 'CUSTOM_DATE',
#             'dateRangeType': date_preset,
            'reportType': 'KEYWORDS_PERFORMANCE_REPORT',
            'downloadFormat': 'CSV',
            'selector': selector
        }
        if not date_preset or date_preset == DatePreset.lifetime:
            self.date_range['min'], self.date_range['max'] = self.ai_start_date, self.ai_stop_date
            self.selector['dateRange'] = self.date_range
            self.report['dateRangeType'] = 'CUSTOM_DATE'
            self.report['selector'] = self.selector
        else:
            self.report['dateRangeType'] = date_preset   
        # Print out the report as a string
        csv =self.report_downloader.DownloadReportAsString(  
            self.report, skip_report_header=True, skip_column_header=True,   
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
        df[df.columns.difference(['Criteria','SystemServingStatus', 'AdGroupType', 'AdGroupStatus', 'BiddingStrategyType', 'Device'])] = df[df.columns.difference(
            ['Criteria','SystemServingStatus', 'AdGroupType', 'AdGroupStatus', 'BiddingStrategyType', 'Device'])].apply(pd.to_numeric, errors='coerce')
        df[df.columns.difference( ['TopImpressionPercentage','Id','Criteria','SystemServingStatus', 'HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'Conversions', 'Ctr'] )] = df[df.columns.difference(
            ['TopImpressionPercentage','Id','Criteria','SystemServingStatus','HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'Conversions', 'Ctr'])].div(1000000)
        df.rename( columns=dict( zip(df.columns, self.db_column_name_list) ), inplace=True )
        self.insights_dict = df.reset_index(drop=True).to_dict(orient='records')
        return self.insights_dict


# In[5]:


def data_collect(customer_id, campaign_id):
    camp = Campaign(customer_id, campaign_id)
    ###
    campaign_lifetime_insights = camp.get_performance_insights( date_preset=datacollector.DatePreset.lifetime, performance_type='CAMPAIGN' )
    if campaign_lifetime_insights.empty:
        return
#     campaign_lifetime_insights = camp.get_campaign_insights( client=None, date_preset=datacollector.DatePreset.today )
    ###
    addition_column_list = [ 'period', 'period_left', 'target', 'target_left', 'daily_target', 'destination', 'destination_type' ]
    period = ( camp.ai_stop_date - camp.ai_start_date ).days + 1
    period_left = ( camp.ai_stop_date-datetime.datetime.now().date() ).days + 1
    if period_left == 0:
        period_left = 1
    target = campaign_lifetime_insights[ datacollector.CAMPAIGN_OBJECTIVE_FIELD[ camp.destination_type ] ]
    target_left = int(camp.destination) - campaign_lifetime_insights[ datacollector.CAMPAIGN_OBJECTIVE_FIELD[ camp.destination_type ] ]
#     try:
    daily_target = target_left / period_left
        
    addition_value_list = [period, period_left, target, target_left, daily_target, camp.destination, camp.destination_type]
    addition_dict = dict( zip(addition_column_list, addition_value_list))
    campaign_dict = {
        **campaign_lifetime_insights.to_dict('records')[0],
        **addition_dict,
    }
    df_campaign = pd.DataFrame(campaign_dict, index=[0])
    print(campaign_lifetime_insights.to_dict('records')[0])
    gsn_db.update_table(df_campaign, table="campaign_target")
    keyword_insights_dict = camp.get_keyword_insights(date_preset='TODAY')
    for keyword_insights in keyword_insights_dict:
        df_keyword_group = pd.DataFrame(keyword_insights, index=[0])
        gsn_db.into_table(df_keyword_group, table="keywords_insights")
        bidding_type = keyword_insights['bidding_type']
        bid_amount_column = datacollector.BIDDING_INDEX[ bidding_type ]
        df_keyword_group['bid_amount'] = df_keyword_group[bid_amount_column]
        df_keyword_group['bid_amount'] = math.ceil(bid_operator.reverse_bid_amount(df_keyword_group[bid_amount_column].iloc[0]))
        gsn_db.check_initial_bid(keyword_insights['keyword_id'], df_keyword_group[['campaign_id', 'adgroup_id', 'keyword_id', 'bid_amount']])
    return


# In[6]:


def main():
    start_time = datetime.datetime.now()
    df_camp = gsn_db.get_campaign_is_running()
    print(df_camp['campaign_id'].unique())
    for campaign_id in df_camp['campaign_id'].unique():
        customer_id = df_camp['customer_id'][df_camp.campaign_id==campaign_id].iloc[0]
        data_collect(customer_id=customer_id, campaign_id=campaign_id)
    print(datetime.datetime.now()-start_time)


# In[2]:


if __name__=='__main__':
    main()
#     data_collect(customer_id=CUSTOMER_ID, campaign_id=CAMPAIGN_ID)


# In[6]:


# !jupyter nbconvert --to script gsn_datacollector.ipynb


# In[ ]:




