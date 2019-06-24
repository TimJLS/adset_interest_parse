#!/usr/bin/env python
# coding: utf-8

# In[40]:


import gdn_datacollector as datacollector
import bid_operator
# from gdn_datacollector import Campaign
# from gdn_datacollector import AdGroup
# from gdn_datacollector import ReportColumn
# from gdn_datacollector import ReportField
# from gdn_datacollector import AUTH_FILE_PATH
# from gdn_datacollector import Field
# from gdn_datacollector import DatePreset
# from gdn_datacollector import CAMPAIGN_OBJECTIVE_FIELD
# from gdn_datacollector import CAMPAIGN_FIELDS
# from gdn_datacollector import ADGROUP_FIELDS
from googleads import adwords
import pandas as pd
import numpy as np
import math
import datetime
import gsn_db

CAMPAIGN_FIELDS = ['ExternalCustomerId','CampaignId', 'AveragePosition','AdvertisingChannelType', 'CampaignStatus',
                   'BiddingStrategyType','Amount','StartDate','EndDate','Cost',
                   'AverageCost','Impressions', 'Clicks','Conversions', 'AllConversions',
                   'AverageCpc','CostPerConversion', 'CostPerAllConversion', 'Ctr']
DB_CAMPAIGN_COLUMN_NAME_LIST = [
    'customer_id', 'campaign_id', 'average_position', 'channel_type', 'status', 'bidding_type', 'daily_budget', 'start_time', 'stop_time',
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


# In[2]:


adwords_client = adwords.AdWordsClient.LoadFromStorage(datacollector.AUTH_FILE_PATH)


# In[15]:


class Campaign(datacollector.Campaign):
    report_metrics = [
        'ExternalCustomerId','CampaignId', 'AdGroupId', 'AdGroupStatus', 'Criteria', 'Id', 'CpmBid', 'CpcBidSource', 'CpcBid',
        'BiddingStrategyType', 'FirstPageCpc','Cost', 'AverageCost','Impressions', 'Clicks','Conversions', 'AverageCpc',
        'CostPerConversion', 'Ctr','AveragePosition', 'SystemServingStatus']
    operand = {
        'field': 'CampaignId',
        'operator': 'EQUALS',
        'values': None
    }
    report = {
        'reportName': None,
        'dateRangeType': 'ALL_TIME',
        'reportType': None,
        'downloadFormat': 'CSV',
        'selector': {
            'fields': None,
            'predicates': [
                operand
            ]
        }
    }
    def __init__(self, customer_id, campaign_id):
        self.customer_id = customer_id
        self.client = adwords.AdWordsClient.LoadFromStorage(datacollector.AUTH_FILE_PATH)
        self.client.SetClientCustomerId(self.customer_id)
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

        self.report['selector']['fields']=fields
      
        if date_preset is None:
            date_preset=datacollector.DatePreset.lifetime
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
            if performance_type=='CAMPAIGN':
                gsn_db.update_table(df, table="campaign_target")
            else:
                gsn_db.into_table( df, performance_type.lower()+'_insights' )
            return df
        
    def get_keyword_id_list(self):
        self.keyword_id_list = []
        # Construct selector and get all ad groups.
        selector = {
            'fields': ['Id', 'KeywordText', 'Status', 'SystemServingStatus'],
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
        page = self.ad_group_criterion_service.get(selector)
        if 'entries' in page:
            for criterion_config in page['entries']:
                if criterion_config['criterionUse'] == 'BIDDABLE' and criterion_config['criterion']['type'] == 'KEYWORD':
                    id = criterion_config['criterion']['id']
                    self.keyword_id_list.append(id)
            return self.keyword_id_list
        
    def get_keyword_insights(self, date_preset='ALL_TIME'):
        self.report_metrics = [
            'ExternalCustomerId','CampaignId', 'AdGroupId', 'AdGroupStatus', 'Criteria', 'Id','AveragePosition', 'SystemServingStatus', 'FirstPageCpc',
            'CpmBid', 'CpcBid', 'BiddingStrategyType', 'AverageCost','Cost','Impressions', 'Clicks','Conversions', 'AverageCpc',
            'CostPerConversion', 'Ctr']
        self.db_column_name_list = [
            'customer_id', 'campaign_id', 'adgroup_id', 'status', 'keyword', 'keyword_id', 'position', 'serving_status', 'first_page_cpc', 'cpm_bid', 'cpc_bid', 'bidding_type', 'cost_per_target', 'spend', 'impressions', 'clicks', 'conversions', 'cost_per_click', 'cost_per_conversion', 'ctr'
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
        df[df.columns.difference( ['AveragePosition','Id','Criteria','SystemServingStatus', 'HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'Conversions', 'Ctr'] )] = df[df.columns.difference(
            ['AveragePosition','Id','Criteria','SystemServingStatus','HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'Conversions', 'Ctr'])].div(1000000)
        df.rename( columns=dict( zip(df.columns, self.db_column_name_list) ), inplace=True )
        self.insights_dict = df.reset_index(drop=True).to_dict(orient='records')
        return self.insights_dict


# In[16]:


class KeywordGroup(object):
    def __init__(self, customer_id, campaign_id, keyword_id):
        self.customer_id = customer_id
        self.campaign_id = campaign_id
        self.keyword_id = keyword_id
        self.client = adwords.AdWordsClient.LoadFromStorage(datacollector.AUTH_FILE_PATH)
        self.client.SetClientCustomerId(self.customer_id)
        self.report_downloader = self.client.GetReportDownloader(version='v201809')
        self.ad_group_criterion_service = self.client.GetService('AdGroupCriterionService', version='v201809')
    
    def get_keyword_insights(self, date_preset='ALL_TIME'):
        self.report_metrics = [
            'ExternalCustomerId','CampaignId', 'AdGroupId', 'AdGroupStatus', 'Criteria', 'Id','AveragePosition', 'SystemServingStatus', 'FirstPageCpc',
            'CpmBid', 'CpcBid', 'BiddingStrategyType', 'AverageCost','Cost','Impressions', 'Clicks','Conversions', 'AverageCpc',
            'CostPerConversion', 'Ctr']
        self.db_column_name_list = [
            'customer_id', 'campaign_id', 'adgroup_id', 'status', 'keyword', 'keyword_id', 'position', 'serving_status', 'first_page_cpc', 'cpm_bid', 'cpc_bid', 'bidding_type', 'cost_per_target', 'spend', 'impressions', 'clicks', 'conversions', 'cost_per_click', 'cost_per_conversion', 'ctr'
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
        df[df.columns.difference( ['AveragePosition','Id','Criteria','SystemServingStatus', 'HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'Conversions', 'Ctr'] )] = df[df.columns.difference(
            ['AveragePosition','Id','Criteria','SystemServingStatus','HourOfDay','Device','ExternalCustomerId','CampaignId','AdGroupType','AdGroupId','AdGroupStatus','BiddingStrategyType','Impressions','Clicks', 'Conversions', 'Ctr'])].div(1000000)
        df.rename( columns=dict( zip(df.columns, self.db_column_name_list) ), inplace=True )
        self.insights_dict = df.reset_index(drop=True).to_dict(orient='records')
        return self.insights_dict

    def get_keyword_criterion(self):
        self.criterion_dict = dict()
        # Construct selector and get all ad groups.
        selector = {
            'fields': ['Id', 'KeywordText', 'Status', 'KeywordMatchType', 'SystemServingStatus', 'FirstPageCpc', 'FirstPositionCpc', 'BidModifier', 'QualityScore'],
            'predicates': [
                {
                    'field': 'CampaignId',
                    'operator': 'EQUALS',
                    'values': [self.campaign_id]
                },
                {
                    'field': 'Id',
                    'operator': 'EQUALS',
                    'values': [self.keyword_id]
                }
            ]
        }
        page = self.ad_group_criterion_service.get(selector)
        if 'entries' in page:
            for criterion_config in page['entries']:
                self.ad_group_id = criterion_config['adGroupId']
                self.text = criterion_config['criterion']['text']
                self.match_type = criterion_config['criterion']['matchType']
                self.serving_status = criterion_config['systemServingStatus']
                self.status = criterion_config['userStatus']
                self.first_page_cpc = criterion_config['firstPageCpc']['amount']['microAmount']/1000000
                self.criterion_dict.update({
                    'ad_group_id':self.ad_group_id, 'text':self.text, 'match_type':self.match_type,
                    'serving_status':self.serving_status, 'status':self.status, 'first_page_cpc':self.first_page_cpc})
            return self.criterion_dict
        
    def update_status(self, status='PAUSED'):
        self.operand = {
        }
        self.operations = [{
            'operator': 'SET',
            'operand': None
        }]
        # Construct operations and update an ad group.
        self.operand['adGroupId'] = self.ad_group_id
        self.operand['xsi_type'] = 'BiddableAdGroupCriterion'
        self.operand['userStatus'] = status
        criterion = {'id':None, 'xsi_type':None}
        criterion['id'] = self.keyword_id
        criterion['xsi_type'] = 'Keyword'
        self.operand['criterion'] = []
        # Put operations > operand > criterion together
        self.operand['criterion'].append(criterion)
        self.operations[0]['operand'] = self.operand
        adgroups = self.ad_group_criterion_service.mutate(self.operations)
        return adgroups
    
    def update_bid(self, bid_micro_amount=None):
        operations = {
            'operator':'SET',
            'operand':None,
        }
        operand = {
            'xsi_type':'BiddableAdGroupCriterion',
            'adGroupId':None,
            'criterion':None,
            'biddingStrategyConfiguration':None,
        }
        criterion = { 'id':self.keyword_id }
        bidding_strategy_config = { 'bids':[] }
        if bid_micro_amount:
            bids = {
                'xsi_type':'CpcBid',
                'bid':{
                    'microAmount': int(bid_micro_amount * 1000000)
                }
            }
            bidding_strategy_config['bids'].append(bids)
        else:
            print('[gsn_datacollector] KeywordGroup.update_bid: bid_micro_amount required.')
            return
        operand['criterion'] = criterion
        operand['adGroupId'] = self.ad_group_id
        operand['biddingStrategyConfiguration'] = bidding_strategy_config
        operations['operand'] = operand
        # Initialize appropriate service.
        ad_groups = self.ad_group_criterion_service.mutate(operations)
        return


# In[36]:


def data_collect(customer_id, campaign_id):
    camp = Campaign(customer_id, campaign_id)
    ###
    campaign_lifetime_insights = camp.get_performance_insights( date_preset=datacollector.DatePreset.today, performance_type='CAMPAIGN' )
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
    keyword_id_list = camp.get_keyword_id_list()
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


# In[37]:


def main():
    start_time = datetime.datetime.now()
    df_camp = gsn_db.get_campaign_is_running()
    print(df_camp['campaign_id'].unique())
    for campaign_id in df_camp['campaign_id'].unique():
        customer_id = df_camp['customer_id'][df_camp.campaign_id==campaign_id].iloc[0]
        data_collect(customer_id=customer_id, campaign_id=campaign_id)
    print(datetime.datetime.now()-start_time)


# In[38]:


if __name__=='__main__':
    main()
#     data_collect(customer_id=CUSTOMER_ID, campaign_id=CAMPAIGN_ID)


# In[39]:


#!jupyter nbconvert --to script gsn_datacollector.ipynb


# In[ ]:




