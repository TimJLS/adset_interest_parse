#!/usr/bin/env python
# coding: utf-8

# In[1]:


import uuid
from googleads import adwords
import sys
import pandas as pd
import numpy as np
import datetime
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
                   'AverageCost','Impressions', 'Clicks','Conversions',
                   'AverageCpc','CostPerConversion']
ADGROUP_FIELDS = ['ExternalCustomerId','CampaignId', 'AdGroupType', 'AdGroupId', 'AdGroupStatus',
                  'CpmBid','CpvBid', 'CpcBid', 'TargetCpa', 'BiddingStrategyType','Cost',
                   'AverageCost','Impressions', 'Clicks','Conversions',
                   'AverageCpc','CostPerConversion']
DB_CAMPAIGN_COLUMN_NAME_LIST = [
    'customer_id', 'campaign_id', 'channel_type', 'status', 'bidding_type', 'daily_budget', 'start_time', 'stop_time', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'cost_per_click', 'cost_per_conversion'
]
DB_ADGROUP_COLUMN_NAME_LIST = [
    'customer_id', 'campaign_id', 'channel_type', 'adgroup_id', 'status', 'cpm_bid', 'cpv_bid', 'cpc_bid', 'cpa_bid', 'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'cost_per_click', 'cost_per_conversion'
]
BIDDING_INDEX = {
    'cpc': 'cpc_bid',
    'cpa': 'cpa_bid',
    'LINK_CLICKS': 'clicks',
    'CONVERSIONS':'conversions',
}


# In[2]:


class Field:
    target_type = 'target_type'
    target = 'target'
    cost_per_target = 'cost_per_target'
    charge_type = 'charge_type'
    start_time = 'start_time'
    stop_time = 'stop_time'
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

class Campaign(object):
    def __init__(self, customer_id, campaign_id, destination_type):
        self.customer_id = customer_id
        self.campaign_id = campaign_id
        self.destination_type = destination_type
        client.SetClientCustomerId(self.customer_id)
        self.report_downloader = client.GetReportDownloader(version='v201809')
    
    def get_adgroup_id_list(self):
        ad_group_service = client.GetService('AdGroupService', version='v201809')
        # Construct selector and get all ad groups.
        selector = {
            'fields': ['Id', 'Name', 'Status'],
            'predicates': [
                {
                    'field': 'CampaignId',
                    'operator': 'EQUALS',
                    'values': [self.campaign_id]
                },
#                 {
#                     'field': 'Status',
#                     'operator': 'EQUALS',
#                     'values': ['ENABLED']
#                 }
            ]
        }
        self.adgroup_id_list = list()
        page = ad_group_service.get(selector)
        if 'entries' in page:
            for ad_group in page['entries']:
                self.adgroup_id_list.append( ad_group['id'] )
        return self.adgroup_id_list
    
    def get_campaign_insights(self, client, date_preset=None):
        if date_preset is None:
            date_preset = 'ALL_TIME'
        else:
            date_preset = date_preset
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
                'predicates': [
                    {
                        'field': 'CampaignId',
                        'operator': 'EQUALS',
                        'values':[self.campaign_id]
                    }
                ]
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
        df[df.columns.difference(['CampaignStatus', 'AdvertisingChannelType', 'BiddingStrategyType', 'StartDate', 'EndDate'])] = df[df.columns.difference(
            ['CampaignStatus', 'AdvertisingChannelType', 'BiddingStrategyType', 'StartDate', 'EndDate'])].apply(pd.to_numeric, errors='coerce')
        df[df.columns.difference(['ExternalCustomerId', 'CampaignId', 'AdvertisingChannelType', 'CampaignStatus', 'BiddingStrategyType', 'StartDate', 'EndDate', 'Impressions', 'Clicks', 'Conversions'])] = df[df.columns.difference(
            ['ExternalCustomerId', 'CampaignId', 'CampaignStatus', 'AdvertisingChannelType', 'BiddingStrategyType', 'StartDate', 'EndDate', 'Impressions', 'Clicks', 'Conversions'])].div(1000000)
        df[['StartDate','EndDate']] = df[['StartDate','EndDate']].apply( pd.to_datetime, errors='coerce' )
        df.rename( columns=dict( zip(df.columns, DB_CAMPAIGN_COLUMN_NAME_LIST) ), inplace=True )
        self.insights_dict = df.to_dict(orient='records')[0]
        return self.insights_dict


# In[3]:


class AdGroup(Campaign):
    def __init__(self, customer_id, campaign_id, destination_type, adgroup_id):
        super().__init__(customer_id, campaign_id, destination_type)
        self.adgroup_id = adgroup_id
        self.report_downloader = client.GetReportDownloader(version='v201809')
        
    def get_adgroup_insights(self, client, date_preset=None):
        
        if date_preset is None:
            date_preset = 'ALL_TIME'
        else:
            date_preset = date_preset
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
                'fields': ADGROUP_FIELDS,
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
                data=np.array(lil_csv.split(',')).reshape(1,len(ADGROUP_FIELDS)),
                columns=ADGROUP_FIELDS
            )
            df = df.append(df_temp)
        df[df.columns.difference(['AdGroupType', 'AdGroupStatus', 'BiddingStrategyType'])] = df[df.columns.difference(
            ['AdGroupType', 'AdGroupStatus', 'BiddingStrategyType'])].apply(pd.to_numeric, errors='coerce')
        df[df.columns.difference( ['ExternalCustomerId', 'CampaignId', 'AdGroupType', 'AdGroupId', 'AdGroupStatus', 'BiddingStrategyType','Impressions','Clicks', 'Conversions'] )] = df[df.columns.difference(
            ['ExternalCustomerId', 'CampaignId', 'AdGroupType', 'AdGroupId', 'AdGroupStatus', 'BiddingStrategyType','Impressions','Clicks', 'Conversions'])].div(1000000)
        df.rename( columns=dict( zip(df.columns, DB_ADGROUP_COLUMN_NAME_LIST) ), inplace=True )
        self.insights_dict = df.reset_index(drop=True).to_dict(orient='records')[0]
        return self.insights_dict

# if __name__ == '__main__':
#     # Initialize client object.
#     client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
# #     adgroup_insights = get_adgroup_insights(client)
#     adgroup_insights = get_adgroup_insights(client, campaign_id=campaign_id)


# In[4]:


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
            'status': 'ENABLED'
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


# In[5]:


def data_collect(customer_id, campaign_id, destination, destination_type):
    camp = Campaign(customer_id, campaign_id, destination_type)
    ###
    campaign_lifetime_insights = camp.get_campaign_insights( client, date_preset=DatePreset.lifetime )
#     campaign_today_insights = camp.get_campaign_insights( client, date_preset=DatePreset.today )
    ###
    addition_column_list = [ 'period', 'period_left', 'target', 'target_left', 'daily_target', 'destination', 'destination_type' ]
    stop_time = campaign_lifetime_insights[ "stop_time" ]
    period = ( campaign_lifetime_insights[ "stop_time" ] - campaign_lifetime_insights[ "start_time" ] ).days + 1
    period_left = ( stop_time-datetime.datetime.now() ).days + 1
    target = campaign_lifetime_insights[ CAMPAIGN_OBJECTIVE_FIELD[ destination_type ] ]
    target_left = int(destination) - campaign_lifetime_insights[ CAMPAIGN_OBJECTIVE_FIELD[ destination_type ] ]
    daily_target = target_left / period_left
    addition_value_list = [period, period_left, target, target_left, daily_target, destination, destination_type]
    addition_dict = dict( zip(addition_column_list, addition_value_list))
    campaign_dict = {
        **campaign_lifetime_insights,
        **addition_dict,
    }
    df_campaign = pd.DataFrame(campaign_dict, index=[0])
#     gdn_db.into_table(df_campaign, table="campaign_target")
    gdn_db.update_table(df_campaign, table="campaign_target")
    adgroup_id_list = camp.get_adgroup_id_list()
    for adgroup_id in adgroup_id_list:
        adgroup = AdGroup(camp.customer_id,camp.campaign_id,camp.destination_type, adgroup_id)
        adgroup_today_insights = adgroup.get_adgroup_insights(client, date_preset=DatePreset.today)
        df_adgroup = pd.DataFrame(adgroup_today_insights, index=[0])
        gdn_db.into_table(df_adgroup, table="adgroup_insights")
        
        bidding_type = BIDDING_INDEX[ destination_type ]
        df_adgroup['bid_amount'] = df_adgroup[bidding_type]
#         df_adgroup['bid_amount'] = math.ceil(reverse_bid_amount(df_adgroup[bidding_type]))
        gdn_db.check_initial_bid(adgroup_id, df_adgroup[[Field.campaign_id, Field.adgroup_id, Field.bid_amount]])
    return df_adgroup


# In[6]:


def main():
    start_time = datetime.datetime.now()
    campaignid_list = gdn_db.get_campaign()['campaign_id'].unique()
    print(campaignid_list)
    for campaign_id in campaignid_list:
        df = gdn_db.get_campaign(campaign_id=campaign_id)
        if len(df) == 0:
            pass
        else:
            print(campaign_id, df['destination_type'])
            data_collect( df['customer_id'].iloc[0], int(campaign_id), df['destination'].iloc[0], df['destination_type'].iloc[0] )
    print(datetime.datetime.now()-start_time)


# In[7]:


if __name__=='__main__':
    main()
#     df_campaign = data_collect(camp.customer_id, camp.campaign_id, 10000, camp.destination_type)


# In[ ]:





# In[ ]:




