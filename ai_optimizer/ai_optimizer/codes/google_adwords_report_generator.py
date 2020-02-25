#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import datetime
import adgeek_permission as permission
import database_controller


# In[ ]:


class DatePreset:
    today = 'TODAY'
    yesterday = 'YESTERDAY'
    lifetime = 'ALL_TIME'
    last_14_days = 'LAST_14_DAYS'
    entire_time = 'ENTIRE_TIME'


# In[ ]:


class Predicates:
    field = 'field'
    operator = 'operator'
    values = 'values'
    def __init__(self):
        self.spec = {
            self.field: None ,
            self.operator: 'EQUALS' ,
            self.values: None
        }


# In[ ]:


class Selector:
    fields = 'fields'
    predicates = 'predicates'
    date_range = 'dateRange'
    def __init__(self):
        self.predicates_object = Predicates()
        self.spec = {
            self.fields: None,
            self.predicates: [self.predicates_object.spec],
            self.date_range: {
                'min': None,
                'max': None
            }
        }


# In[ ]:


class Operator:
    pass


# In[ ]:


class Report:
    report_name = 'reportName'
    report_type = 'reportType'
    download_format = 'downloadFormat'
    selector = 'selector'
    date_range_type = 'dateRangeType'
    def __init__(self):
        self.selector_object = Selector()
        self.spec = {
            self.report_name: None,
            self.report_type: None,
            self.download_format: 'CSV',
            self.selector: self.selector_object.spec,
            self.date_range_type: 'CUSTOM_DATE'
        }


# In[ ]:


class ReportGenerator(object):
    _non_numeric_columns = [
        'Criterion serving status', 'Ad group type', 'Ad group state', 'Bid Strategy Type', 'Keyword', 'Age Range', 'Audience',
        'Keyword / Placement', 'Criteria Display Name', 'Topic'
    ]
    _money_columns = [
        'First page CPC', 'Max. CPM', 'Max. CPV', 'Max. CPC', 'Default max. CPC', 'Cost', 'Avg. Cost', 'Avg. CPC',
        'Cost / conv.', 'Cost / all conv.', 'Amount', 'AverageCost', 'AverageCpc', 'CostPerAllConversion'
    ]
    param_types = {
        'breakdowns': ['hour', 'day', 'device', 'ad_group']
    }
    google_smart_bidding = {
        'Maximize clicks': 'CpcBid',
        'Maximize Conversions': 'CpcBid',
        'Target CPA': 'TargetCpa',
        'Target ROAS': None,
        'Maximize Conversion Value': None,
        'Target Outranking Share': None,
    }
    def __init__(self, campaign_id, media):
        self.campaign_id = campaign_id
        self.__init_customer(media)
        self.client = permission.init_google_api(self.customer_id)
        self.report_downloader = self.client.GetReportDownloader(version='v201809')
        self.__init_brief()
        self.__init_report()
        self.__init_predicates_object()

    def __init_customer(self, media):
        global database
        if media.lower() == 'gsn':
            database = database_controller.GSN(database_controller.Database)
        elif media.lower() == 'gdn':
            database = database_controller.GDN(database_controller.Database)
        else: raise ValueError("arg media should be 'gdn' or gsn")
        campaign = database.get_one_campaign(campaign_id=self.campaign_id).to_dict('records')[0]
        self.customer_id = campaign['customer_id']

    def __init_brief(self):
        self.brief = database.get_brief(campaign_id=self.campaign_id)
        self.ai_start_date = self.brief['ai_start_date'].strftime("%Y%m%d")
        self.ai_stop_date = self.brief['ai_stop_date'].strftime("%Y%m%d")
        self.ai_spend_cap = self.brief['ai_spend_cap']
        self.destination_type = self.brief['destination_type']

    def __init_report(self):
        self.report = Report()

    def _init_fields(self):
        self.fields = self._fields.copy()
        self.columns = self._columns.copy()

    def __init_predicates_object(self):
        self.report.selector_object.predicates_object.spec['field'] = 'CampaignId'
        self.report.selector_object.predicates_object.spec['values'] = self.campaign_id

    def __init_selector(self, date_preset=None):
        self.date_preset = date_preset
        self.report.selector_object.spec['fields'] = self.fields
        if not date_preset or date_preset == 'ALL_TIME':
            self.report.selector_object.spec['dateRange']['min'] = self.ai_start_date
            self.report.selector_object.spec['dateRange']['max'] = self.ai_stop_date
            
        elif date_preset.upper() == 'ENTIRE_TIME':
            unix_time = datetime.date(1970, 1, 1)
            self.report.selector_object.spec['dateRange']['min'] = unix_time.strftime("%Y%m%d")
            self.report.selector_object.spec['dateRange']['max'] = datetime.date.today().strftime("%Y%m%d")
            
        elif date_preset.upper() == 'LAST_7_DAYS':
            last_7_days = datetime.date.today() - datetime.timedelta(days=7)
            last_7_days = last_7_days if last_7_days > self.ai_start_date else self.ai_start_date
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            self.report.selector_object.spec['dateRange']['min'] = last_7_days.strftime("%Y%m%d")
            self.report.selector_object.spec['dateRange']['max'] = yesterday.strftime("%Y%m%d")

        elif date_preset.upper() == 'YESTERDAY':
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            self.report.selector_object.spec['dateRange']['min'] = yesterday.strftime("%Y%m%d")
            self.report.selector_object.spec['dateRange']['max'] = yesterday.strftime("%Y%m%d")

        else:
            self.report.selector_object.spec['dateRange']['min'] = datetime.date.today().strftime("%Y%m%d")
            self.report.selector_object.spec['dateRange']['max'] = datetime.date.today().strftime("%Y%m%d")

    def _extract_value(self, value):
        if value == 'day':
            self.fields.append('Date')
            self.columns.append('Date')
        elif value == 'hour':
            self.fields.append('HourOfDay')
            self.columns.append('hour_of_day')
            # breakdown 'hour' is not compatible with 'AllConversions' & 'CostPerAllConversion'
            self.fields.remove('AllConversions'), self.fields.remove('CostPerAllConversion')
            self.columns.remove('all_conversions'), self.columns.remove('cost_per_all_conversion')
        elif value == 'device':
            self.fields.append('Device')
            self.columns.append('device')
        elif value == 'ad_group':
            self.fields.append('AdGroupId')
            self.columns.append('ad_group_id')

    def add_param(self, key, value):
        if key in self.param_types.keys():
            for val in value:
                if val in self.param_types[key]:
                    self._extract_value(val)
        return self

    def add_params(self, params):
        self._init_fields()
        if params is None:
            return self
        for key in params.keys():
            self.add_param(key, params[key])
        return self

    def get_insights(self, date_preset, params=None):
        self.add_params(params)
        self.__init_selector(date_preset)
        data = self.report_downloader.DownloadReportAsString(self.report.spec,
                                                             skip_report_header=True,
                                                             skip_column_header=True,
                                                             skip_report_summary=True,
                                                             include_zero_impressions=True,
                                                             client_customer_id=self.customer_id)
        data_list = data.split('\n')[:-1]
        data_df = pd.DataFrame( columns=self.fields, data=[data.split(',') for data in data_list] )
        if data_df.empty:
            return []
        self.bidding_type = data_df['BiddingStrategyType'].all()
        if 'Ctr' in data_df.columns:
            data_df['Ctr'] = data_df.Ctr.str.split('%', expand = True)[0]
        data_df[data_df.columns.difference( self._non_numeric_columns )] = data_df[data_df.columns.difference( self._non_numeric_columns )].apply(pd.to_numeric, errors='ignore')
        numeric_money_list = [column for column in data_df.columns if (column in self._money_columns) and data_df[column].dtype != 'object']
        data_df[data_df.columns.intersection( numeric_money_list )] = data_df[data_df.columns.intersection( numeric_money_list )].div(1000000)
        data_df.rename( columns=dict(zip(data_df.columns, self.columns)), inplace=True)
        return [data.to_dict() for idx, data in data_df.iterrows()]


# In[ ]:


class CampaignReportGenerator(ReportGenerator):
    _fields = [
        'ExternalCustomerId', 'CampaignId', 'AdvertisingChannelType', 'CampaignStatus', 'BiddingStrategyType', 'Amount', 'Cost',
        'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion',
        'Ctr', 'ViewThroughConversions'
    ]
    _columns = [
        'customer_id', 'campaign_id', 'channel_type', 'status', 'bidding_type', 'daily_budget', 'spend',
        'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion',
        'cost_per_all_conversion', 'ctr', 'view_conversions'
    ]
    report_name = 'CAMPAIGN_PERFORMANCE_REPORT'
    report_type = 'CAMPAIGN_PERFORMANCE_REPORT'
    
    def __init__(self, campaign_id, media):
        super().__init__(campaign_id, media)
        self.fields = CampaignReportGenerator._fields.copy()
        self.columns = CampaignReportGenerator._columns.copy()
        self.__init_report()
#         self.__init_predicates_object()
        
    def __init_report(self):
        self.report.spec[self.report.report_name] = self.report_name
        self.report.spec[self.report.report_type] = self.report_type


# In[ ]:


class AdScheduleReportGenerator(ReportGenerator):
    _fields = [
        'ExternalCustomerId', 'CampaignId', 'CampaignStatus', 'Cost', 'Id', 'BidModifier',
        'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion',
        'Ctr', 'ViewThroughConversions'
    ]
    _columns = [
        'customer_id', 'campaign_id', 'status', 'spend', 'id', 'bid_modifier',
        'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion',
        'cost_per_all_conversion', 'ctr', 'view_conversions'
    ]
    report_name = 'CAMPAIGN_AD_SCHEDULE_TARGET_REPORT'
    report_type = 'CAMPAIGN_AD_SCHEDULE_TARGET_REPORT'

    def __init__(self, campaign_id, media):
        super().__init__(campaign_id, media)
        self._fields = self._fields
        self._columns = self._columns
        self.__init_report()

    def __init_report(self):
        self.report.spec[self.report.report_name] = self.report_name
        self.report.spec[self.report.report_type] = self.report_type
        
    def get_insights(self, date_preset, params=None):
        self.add_params(params)
        self._ReportGenerator__init_selector(date_preset)
        data = self.report_downloader.DownloadReportAsString(self.report.spec,
                                                             skip_report_header=True,
                                                             skip_column_header=True,
                                                             skip_report_summary=True,
                                                             include_zero_impressions=True,
                                                             client_customer_id=self.customer_id)
        data_list = data.split('\n')[:-1]
        data_df = pd.DataFrame( columns=self.fields, data=[data.split(',') for data in data_list] )
        data_df.rename( columns=dict(zip(data_df.columns, self.columns)), inplace=True)
        data_df = data_df.apply(pd.to_numeric, errors='ignore')
        week_map = dict(zip([x for x in range(30, 37)], ['mon','tue','wed','thr','fri','sat','sun']))
        div, mod = divmod(data_df['id'], pow(10, 4))
        data_df['weekofday'] = div.map(lambda value: week_map[value])
        data_df['starthour'], data_df['endhour'] = divmod((mod // 4), 100)
        return [data.to_dict() for idx, data in data_df.iterrows()]


# In[ ]:


class AdGroupReportGenerator(ReportGenerator):
    _fields = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupType', 'AdGroupId', 'AdGroupStatus', 'CpmBid', 'CpvBid', 'CpcBid', 'TargetCpa',
        'EnhancedCpcEnabled', 'BiddingStrategyType', 'Cost', 'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions',
        'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'Ctr', 'ViewThroughConversions'
    ]
    _columns = [
        'customer_id', 'campaign_id', 'channel_type', 'adgroup_id', 'status', 'cpm_bid', 'cpv_bid', 'cpc_bid', 'cpa_bid', 'enhanced',
        'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click',
        'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 'view_conversions' 
    ]
    report_name = 'ADGROUP_PERFORMANCE_REPORT'
    report_type = 'ADGROUP_PERFORMANCE_REPORT'
    
    def __init__(self, campaign_id, media):
        super().__init__(campaign_id, media)
        self._fields = self._fields
        self._columns = self._columns
        self.__init_report()
        
    def __init_report(self):
        self.report.spec[self.report.report_name] = self.report_name
        self.report.spec[self.report.report_type] = self.report_type
        
    def get_bidding_column(self, data):
        if self.bidding_type in self.google_smart_bidding.keys() and self.google_smart_bidding[self.bidding_type] in data.columns:
            self.col = self.google_smart_bidding.get(self.bidding_type)
        else:
            self.col = 'CpcBid'
        return data
    
    def parse_bidding_column(self, data):
        if self.bidding_type in ['Maximize clicks', 'Maximize Conversions']:
            data[self.col] = data[self.col].str.split(':', expand = True)[1]
        else:
            pass
        
        return data
    
    def get_insights(self, date_preset, params=None):
        self.add_params(params)
        self._ReportGenerator__init_selector(date_preset)
        data = self.report_downloader.DownloadReportAsString(self.report.spec,
                                                             skip_report_header=True,
                                                             skip_column_header=True,
                                                             skip_report_summary=True,
                                                             include_zero_impressions=True,
                                                             client_customer_id=self.customer_id)
        data_list = data.split('\n')[:-1]
        data_df = pd.DataFrame( columns=self.fields, data=[data.split(',') for data in data_list] )
#         return data_df
        if data_df.empty:
            return []
        self.bidding_type = data_df['BiddingStrategyType'].all()
        if 'Ctr' in data_df.columns:
            data_df['Ctr'] = data_df.Ctr.str.split('%', expand = True)[0]
        data_df = self.parse_bidding_column(self.get_bidding_column(data_df))
        data_df[data_df.columns.difference( self._non_numeric_columns )] = data_df[data_df.columns.difference( self._non_numeric_columns )].apply(pd.to_numeric, errors='ignore')
        numeric_money_list = [column for column in data_df.columns if (column in self._money_columns+[self.col]) and data_df[column].dtype != 'object']
        data_df[data_df.columns.intersection( numeric_money_list )] = data_df[data_df.columns.intersection( numeric_money_list )].div(1000000)
        data_df.rename( columns=dict(zip(data_df.columns, self.columns)), inplace=True)
        return [data.to_dict() for idx, data in data_df.iterrows()]


# In[ ]:


class AdReportGenerator(ReportGenerator):
    _fields = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'Id', 'AdGroupStatus', 'Cost',
        'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc',
        'CostPerConversion', 'CostPerAllConversion', 'Ctr', 'ViewThroughConversions'
    ]
    _columns = [
        'customer_id', 'campaign_id', 'adgroup_id', 'ad_id', 'status', 'spend', 'cost_per_target',
        'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click',
        'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 'view_conversions'
    ]
    report_name = 'AD_PERFORMANCE_REPORT'
    report_type = 'AD_PERFORMANCE_REPORT'
    
    def __init__(self, campaign_id, media):
        super().__init__(campaign_id, media)
        self._fields = self._fields
        self._columns = self._columns
        self.__init_report()
        
    def __init_report(self):
        self.report.spec[self.report.report_name] = self.report_name
        self.report.spec[self.report.report_type] = self.report_type
        
    def get_bidding_column(self, data):
        if self.bidding_type in self.google_smart_bidding.keys() and self.google_smart_bidding[self.bidding_type] in data.columns:
            self.col = self.google_smart_bidding.get(self.bidding_type)
        else:
            self.col = 'CpcBid'
        return data
    
    def parse_bidding_column(self, data):
        if self.bidding_type in ['Maximize clicks', 'Maximize Conversions']:
            data[self.col] = data[self.col].str.split(':', expand = True)[1]
        return data
    
    def get_insights(self, date_preset, params=None):
        self.add_params(params)
        self._ReportGenerator__init_selector(date_preset)
        data = self.report_downloader.DownloadReportAsString(self.report.spec,
                                                             skip_report_header=True,
                                                             skip_column_header=True,
                                                             skip_report_summary=True,
                                                             include_zero_impressions=True,
                                                             client_customer_id=self.customer_id)
        data_list = data.split('\n')[:-1]
        data_df = pd.DataFrame( columns=self.fields, data=[data.split(',') for data in data_list] )
        if data_df.empty:
            return []
        if 'Ctr' in data_df.columns:
            data_df['Ctr'] = data_df.Ctr.str.split('%', expand = True)[0]
        data_df[data_df.columns.difference( self._non_numeric_columns )] = data_df[data_df.columns.difference( self._non_numeric_columns )].apply(pd.to_numeric, errors='ignore')
        data_df[data_df.columns.intersection( self._money_columns )] = data_df[data_df.columns.intersection( self._money_columns )].div(1000000)
        data_df.rename( columns=dict(zip(data_df.columns, self.columns)), inplace=True)
        return [data.to_dict() for idx, data in data_df.iterrows()]


# In[ ]:


class KeywordReportGenerator(ReportGenerator):
    _fields = [
        'ExternalCustomerId','CampaignId', 'AdGroupId', 'AdGroupStatus', 'Criteria', 'Id','TopImpressionPercentage', 'SystemServingStatus',
        'FirstPageCpc', 'CpmBid', 'CpcBid', 'EnhancedCpcEnabled', 'BiddingStrategyType', 'AverageCost', 'Cost', 
        'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'Ctr', 
        'ViewThroughConversions'
    ]
    _columns = [
        'customer_id', 'campaign_id', 'adgroup_id', 'status', 'keyword', 'keyword_id', 'top_impression_percentage', 'serving_status',
        'first_page_cpc', 'cpm_bid', 'cpc_bid', 'enhanced', 'bidding_type', 'cost_per_target', 'spend', 'impressions',
        'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 
        'view_conversions' 
    ]
    report_name = 'KEYWORDS_PERFORMANCE_REPORT'
    report_type = 'KEYWORDS_PERFORMANCE_REPORT'
    
    def __init__(self, campaign_id, media):
        super().__init__(campaign_id, media)
        self._fields = self._fields
        self._columns = self._columns
        self.__init_report()
        
    def __init_report(self):
        self.report.spec[self.report.report_name] = self.report_name
        self.report.spec[self.report.report_type] = self.report_type

    def get_bidding_column(self, data):
        if self.bidding_type in self.google_smart_bidding.keys() and self.google_smart_bidding[self.bidding_type] in data.columns:
            self.col = self.google_smart_bidding.get(self.bidding_type)
        else:
            self.col = 'CpcBid'
        return data
    
    def parse_bidding_column(self, data):
        if self.bidding_type in ['Maximize clicks', 'Maximize Conversions']:
            data[self.col] = data[self.col].str.split(':', expand = True)[1]
        else:
            pass
        return data
    
    def get_insights(self, date_preset, params=None):
        self.add_params(params)
        self._ReportGenerator__init_selector(date_preset)
        data = self.report_downloader.DownloadReportAsString(self.report.spec,
                                                             skip_report_header=True,
                                                             skip_column_header=True,
                                                             skip_report_summary=True,
                                                             include_zero_impressions=True,
                                                             client_customer_id=self.customer_id)
        data_list = data.split('\n')[:-1]
        data_df = pd.DataFrame( columns=self.fields, data=[data.split(',') for data in data_list] )
        if data_df.empty:
            return []
        self.bidding_type = data_df['BiddingStrategyType'].all()
        if 'Ctr' in data_df.columns:
            data_df['Ctr'] = data_df.Ctr.str.split('%', expand = True)[0]
        data_df = self.parse_bidding_column(self.get_bidding_column(data_df))
        data_df[data_df.columns.difference( self._non_numeric_columns )] = data_df[data_df.columns.difference( self._non_numeric_columns )].apply(pd.to_numeric, errors='ignore')
        numeric_money_list = [column for column in data_df.columns if (column in self._money_columns+[self.col]) and data_df[column].dtype != 'object']
        data_df[data_df.columns.intersection( numeric_money_list )] = data_df[data_df.columns.intersection( numeric_money_list )].div(1000000)
        data_df.rename( columns=dict(zip(data_df.columns, self.columns)), inplace=True)
        return [data.to_dict() for idx, data in data_df.iterrows()]


# In[ ]:


class AudienceReportGenerator(ReportGenerator):
    _fields = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'Criteria', 'Id', 'AdGroupStatus', 'CpmBid', 'CpcBid', 'BiddingStrategyType', 
        'Cost', 'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 
        'CostPerAllConversion', 'Ctr', 'ViewThroughConversions'
    ]
    _columns = [
        'customer_id', 'campaign_id', 'adgroup_id', 'audience', 'criterion_id', 'status', 'cpm_bid', 'cpc_bid', 'bidding_type', 'spend', 
        'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion', 
        'cost_per_all_conversion', 'ctr', 'view_conversions'
    ]
    report_name = 'AUDIENCE_PERFORMANCE_REPORT'
    report_type = 'AUDIENCE_PERFORMANCE_REPORT'
    
    def __init__(self, campaign_id, media):
        super().__init__(campaign_id, media)
        self._fields = self._fields
        self._columns = self._columns
        self.__init_report()
        
    def __init_report(self):
        self.report.spec[self.report.report_name] = self.report_name
        self.report.spec[self.report.report_type] = self.report_type


# In[ ]:


class DisplayTopicReportGenerator(ReportGenerator):
    _fields = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'Criteria', 'Id', 'VerticalId', 'AdGroupStatus', 'CpmBid', 'CpcBid',
        'BiddingStrategyType', 'Cost', 'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc',
        'CostPerConversion', 'CostPerAllConversion', 'Ctr', 'ViewThroughConversions'
    ]
    _columns = [
        'customer_id', 'campaign_id', 'adgroup_id', 'topics', 'criterion_id', 'vertical_id', 'status', 'cpm_bid', 'cpc_bid',
        'bidding_type', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 
        'cost_per_conversion', 'cost_per_all_conversion', 'ctr', 'view_conversions'
    ]
    report_name = 'DISPLAY_TOPICS_PERFORMANCE_REPORT'
    report_type = 'DISPLAY_TOPICS_PERFORMANCE_REPORT'
    
    def __init__(self, campaign_id, media):
        super().__init__(campaign_id, media)
        self._fields = self._fields
        self._columns = self._columns
        self.__init_report()
        
    def __init_report(self):
        self.report.spec[self.report.report_name] = self.report_name
        self.report.spec[self.report.report_type] = self.report_type


# In[ ]:


class DisplayKeywordReportGenerator(ReportGenerator):
    _fields = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'Criteria', 'Id', 'AdGroupStatus', 'CpmBid', 'CpcBid', 'BiddingStrategyType', 
        'Cost', 'AverageCost', 'Impressions', 'Clicks', 'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 
        'CostPerAllConversion', 'Ctr', 'ViewThroughConversions'
    ]
    _columns = [
        'customer_id', 'campaign_id', 'adgroup_id', 'keyword', 'keyword_id', 'status', 'cpm_bid', 'cpc_bid', 'bidding_type', 'spend', 
        'cost_per_target', 'impressions', 'clicks', 'conversions', 'all_conversions', 'cost_per_click', 'cost_per_conversion',
        'cost_per_all_conversion', 'ctr', 'view_conversions'
    ]
    report_name = 'DISPLAY_KEYWORD_PERFORMANCE_REPORT'
    report_type = 'DISPLAY_KEYWORD_PERFORMANCE_REPORT'
    
    def __init__(self, campaign_id, media):
        super().__init__(campaign_id, media)
        self._fields = self._fields
        self._columns = self._columns
        self.__init_report()
        
    def __init_report(self):
        self.report.spec[self.report.report_name] = self.report_name
        self.report.spec[self.report.report_type] = self.report_type


# In[ ]:


class UrlReportGenerator(ReportGenerator):
    _fields = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'AdGroupStatus', 'Cost', 'AverageCost', 'Impressions', 'Clicks',
        'Conversions', 'AllConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion', 'DisplayName', 'Ctr',
        'ViewThroughConversions'
    ]
    _columns = [
        'customer_id', 'campaign_id', 'adgroup_id', 'status', 'spend', 'cost_per_target', 'impressions', 'clicks', 'conversions',
        'all_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion', 'url_display_name', 'ctr',
        'view_conversions'
    ]
    report_name = 'URL_PERFORMANCE_REPORT'
    report_type = 'URL_PERFORMANCE_REPORT'
    
    def __init__(self, campaign_id, media):
        super().__init__(campaign_id, media)
        self.fields = self._fields
        self.columns = self._columns
        self.__init_report()
        
    def __init_report(self):
        self.report.spec[self.report.report_name] = self.report_name
        self.report.spec[self.report.report_type] = self.report_type
        
    def get_insights(self, date_preset):
        self._ReportGenerator__init_selector(date_preset)
        data = self.report_downloader.DownloadReportAsString(self.report.spec,
                                                             skip_report_header=True,
                                                             skip_column_header=True,
                                                             skip_report_summary=True,
                                                             include_zero_impressions=False,
                                                             client_customer_id=self.customer_id)
        data_list = data.split('\n')[:-1]
        data_df = pd.DataFrame( columns=self._fields, data=[data.split(',') for data in data_list] )
        if data_df.empty:
            return []
        if 'Ctr' in data_df.columns:
            data_df['Ctr'] = data_df.Ctr.str.split('%', expand = True)[0]
        data_df[data_df.columns.difference( self._non_numeric_columns )] = data_df[data_df.columns.difference( self._non_numeric_columns )].apply(pd.to_numeric, errors='ignore')
        data_df[data_df.columns.intersection( self._money_columns )] = data_df[data_df.columns.intersection( self._money_columns )].div(1000000)
        data_df = data_df.groupby('DisplayName').agg({'ExternalCustomerId': lambda x: x.mean(),
                                                      'CampaignId': lambda x: x.mean(),
                                                      'Cost': 'sum',
                                                      'Impressions': 'sum',
                                                      'Clicks': 'sum',
                                                      'AllConversions': 'sum',
                                                      'Conversions': 'sum',
                                                       'ViewThroughConversions': 'sum',
                                                      'Ctr': lambda x: (data_df.loc[x.index].Clicks.sum())/(data_df.loc[x.index].Impressions.sum())*100,
                                                     }).reset_index()
        data_df.rename( columns=dict(zip(self._fields, self._columns)), inplace=True)
        return [data.to_dict() for idx, data in data_df.iterrows()]


# In[ ]:


class PlacementReportGenerator(ReportGenerator):
    _fields = [
        'ExternalCustomerId', 'CampaignId', 'DisplayName', 'Cost', 'Ctr', 'Impressions', 'Clicks', 'AllConversions',
        'Conversions', 'ViewThroughConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion']
    _columns = [
        'customer_id', 'campaign_id', 'display_name', 'spend', 'ctr', 'impressions', 'clicks', 'all_conversions',
        'conversions', 'view_conversions', 'cost_per_click', 'cost_per_conversion', 'cost_per_all_conversion',
    ]
    report_name = 'PLACEMENT_PERFORMANCE_REPORT'
    report_type = 'PLACEMENT_PERFORMANCE_REPORT'
    
    def __init__(self, campaign_id, media):
        super().__init__(campaign_id, media)
        self.__init_report()
        self.fields = self._fields.copy()
        self.columns = self._columns.copy()
        
    def __init_report(self):
        self.report.spec[self.report.report_name] = self.report_name
        self.report.spec[self.report.report_type] = self.report_type
        
    def get_insights(self, date_preset, params=None):
        self.add_params(params)
        self._ReportGenerator__init_selector(date_preset)
        data = self.report_downloader.DownloadReportAsString(self.report.spec,
                                                             skip_report_header=True,
                                                             skip_column_header=True,
                                                             skip_report_summary=True,
                                                             include_zero_impressions=False,
                                                             client_customer_id=self.customer_id)
        data_list = data.split('\n')[:-1]
        data_df = pd.DataFrame( columns=self.fields, data=[data.split(',') for data in data_list] )
        if data_df.empty:
            return []
        if 'Ctr' in data_df.columns:
            data_df['Ctr'] = data_df.Ctr.str.split('%', expand = True)[0]
        data_df[data_df.columns.difference( self._non_numeric_columns )] = data_df[
            data_df.columns.difference( self._non_numeric_columns )].apply(pd.to_numeric, errors='ignore')
        data_df[data_df.columns.intersection( self._money_columns )] = data_df[
            data_df.columns.intersection( self._money_columns )].div(1000000)
        groupby_list = ['DisplayName']
        agg_dict = {
            'ExternalCustomerId': lambda x: x.mean(),
            'CampaignId': lambda x: x.mean(),
            'Cost': 'sum',
            'Impressions': 'sum',
            'Clicks': 'sum',
            'AllConversions': 'sum',
            'Conversions': 'sum',
            'ViewThroughConversions': 'sum',
            'Ctr': lambda x: (data_df.loc[x.index].Clicks.sum())/(data_df.loc[x.index].Impressions.sum())*100,
        }
        if 'ad_group' in params['breakdowns']:
            groupby_list.append('AdGroupId')
        data_df = data_df.groupby(by=groupby_list).agg(agg_dict).reset_index()
        data_df.rename( columns=dict(zip(self._fields, self._columns)), inplace=True)
        return [data.to_dict() for idx, data in data_df.iterrows()]


# In[ ]:


class SearchKeywordReportGenerator(ReportGenerator):
    _fields = [
        'ExternalCustomerId', 'CampaignId', 'AdGroupId', 'Query', 'QueryTargetingStatus', 'KeywordTextMatchingQuery',
        'KeywordId', 'QueryMatchTypeWithVariant', 'Impressions', 'Cost', 'Ctr', 'Clicks', 'AllConversions', 'Conversions',
        'ViewThroughConversions', 'AverageCpc', 'CostPerConversion', 'CostPerAllConversion'
    ]
    _columns = [
        'customer_id', 'campaign_id', 'adgroup_id', 'query', 'status', 'keyword_matching_query', 'keyword_id', 'query_matching_variant',
        'impressions', 'spend', 'ctr', 'clicks', 'all_conversions', 'conversions', 'view_conversions', 'cost_per_click',
        'cost_per_conversion', 'cost_per_all_conversion'
    ]
    report_name = 'SEARCH_QUERY_PERFORMANCE_REPORT'
    report_type = 'SEARCH_QUERY_PERFORMANCE_REPORT'
    
    def __init__(self, campaign_id, media):
        super().__init__(campaign_id, media)
        self.fields = self._fields.copy()
        self.columns = self._columns.copy()
        self.__init_report()
        
    def __init_report(self):
        self.report.spec[self.report.report_name] = self.report_name
        self.report.spec[self.report.report_type] = self.report_type
        
    def get_insights(self, date_preset):
        self._ReportGenerator__init_selector(date_preset)
        data = self.report_downloader.DownloadReportAsString(self.report.spec,
                                                             skip_report_header=True,
                                                             skip_column_header=True,
                                                             skip_report_summary=True,
                                                             include_zero_impressions=False,
                                                             client_customer_id=self.customer_id)
        data_list = data.split('\n')[:-1]
        data_df = pd.DataFrame( columns=self._fields, data=[data.split(',') for data in data_list] )
        data_df[data_df.columns.difference( self._non_numeric_columns )] = data_df[
            data_df.columns.difference( self._non_numeric_columns )].apply(pd.to_numeric, errors='ignore')
        data_df[data_df.columns.intersection( self._money_columns )] = data_df[
            data_df.columns.intersection( self._money_columns )].div(1000000)
        data_df.rename( columns=dict(zip(self._fields, self._columns)), inplace=True)
        return [data.to_dict() for idx, data in data_df.iterrows()]


# In[ ]:


def main():
    global database
    database = database_controller.GDN(database_controller.Database)


# In[ ]:


# if __name__=='__main__':
#     main()


# In[ ]:


# !jupyter nbconvert --to script google_adwords_report_generator.ipynb


# In[ ]:




