#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
sys.path.append('../')
import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import statsmodels.api as sm
import seaborn as sns
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from loguru import logger

import facebook_business.adobjects.campaign as campaign
import facebook_business.adobjects.adset as adset

import adgeek_permission as permission
import facebook_datacollector as data_collector
import database_controller
LEN_SEQUENCE = 5
PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/auto_calibration/facebook/{date}.log'.format(
    date=datetime.datetime.strftime(datetime.datetime.today(), "%m_%d_%Y"))
logger.add(PATH)


# In[ ]:


class TimeSegments:
    Day = {'time_increment': 1,}
    Hour = {'breakdowns':['hourly_stats_aggregated_by_audience_time_zone'],}


# In[ ]:


class Insights(object):
    _fields = ['impressions', 'reach', 'cpm', 'frequency']
    def __init__(self, campaign_id=None, adset_id=None, time_segment='Day', date_preset=None):
        self.campaign_id = campaign_id
        self.adset_id = adset_id
        self.time_segment = time_segment
        self.date_preset = date_preset
        self.is_available = True
        self.origin_bid = 0
        self.sequence = 0
        self._api_init()
        self._retrieve()
        self._get_bid()
        self._parse()
        
    def _api_init(self):
        brief_campaign = database.get_one_campaign(campaign_id=self.campaign_id)
        if brief_campaign.empty: raise ValueError("This campaign doesn't exist.")
        self.account_id = brief_campaign['account_id'].values[0].astype(object)
        permission.init_facebook_api(self.account_id)

    def _set_time_segment(self):
        self.params = dict()
        if self.time_segment in ['Day', 'day']:
            self.params.update(TimeSegments.Day)
        elif self.time_segment in ['Hour', 'hour']:
            self.params.update(TimeSegments.Hour)

    def _get_bid(self):
        df_insights = database.retrieve("table_insights", adset_id=self.adset_id)
        if df_insights.empty:
            self.is_available = False
            return
        self.df_bid = df_insights.set_index('request_time').resample('d').mean()[['bid_amount']]

    def _retrieve(self):
        self._set_time_segment()
        if self.adset_id:
            self.raw_insights = list(adset.AdSet(self.adset_id).get_insights(params=self.params, fields=self._fields))
        elif self.campaign_id:
            self.raw_insights = list(campaign.Campaign(self.campaign_id).get_insights(params=self.params, fields=self._fields))
        return self.raw_insights

    def _parse(self):
        df = pd.DataFrame(self.raw_insights)
        if df.empty or len(df) < LEN_SEQUENCE:
            self.sequence = len(df)
            self.is_available = False
            return
        df = df[df.cpm!='0']
        df[['date_start', 'date_stop']] = df[['date_start', 'date_stop']].apply(pd.to_datetime, errors='coerce')
        df[['impressions', 'reach', 'cpm', 'frequency']] = df[['impressions', 'reach', 'cpm', 'frequency']].apply(pd.to_numeric, errors='coerce')
        df['cpr'] = df['cpm'] * df['frequency']
        self.df_insights = df.set_index('date_start')
        self.df_insights = self.df_insights.reindex(
            pd.date_range(start=self.df_insights.index.min(),
                          end=self.df_insights.index.max(),
                          freq='D'),
            fill_value=np.nan
        )
        self.df_bid = self.df_bid.reindex(
            pd.date_range(start=self.df_bid.index.min(),
                          end=self.df_bid.index.max(),
                          freq='D'),
            fill_value=np.nan
        )
        self.df_insights = self.df_insights.join(self.df_bid).interpolate()
        self.df_insights = self.df_insights[self.df_insights.bid_amount != 0].dropna(subset=['bid_amount'])
        self.sequence = len(self.df_insights)
        self.origin_bid = self.df_insights.tail(1).bid_amount.values[0]
        return self.df_insights

    def polynomial_fit(self, degree=2):
        assertion_message = logger.debug("bidding length != cpm length")
        assert len(self.df_insights.bid_amount.values) == len(self.df_insights.cpm.values), assertion_message
        index_series = np.array([x for x in range(0, len(self.df_insights.bid_amount.values)+1)])
        # init scaler
        cpm_scaler = MinMaxScaler()
        bid_scaler = MinMaxScaler()#feature_range=(1, 10000))
        # first fit cpm
        cpm_scaler.fit(self.df_insights.bid_amount.values.reshape(-1, 1))
        scaled_cpm = cpm_scaler.fit_transform(self.df_insights.cpm.values.reshape(-1, 1))
        # fit bid amount data
        bid_scaler.fit(self.df_insights.cpm.values.reshape(-1, 1))
        scaled_bid = bid_scaler.fit_transform(self.df_insights.bid_amount.values.reshape(-1, 1))

        reg = np.polyfit(index_series[:-1], scaled_cpm.reshape(-1), degree)
        scaled = np.polyval(reg, np.array([index_series[-1]]))
        revert_bid = bid_scaler.inverse_transform(scaled.reshape(-1, 1))
        self.reverted_bid = revert_bid.reshape(-1)[0]
        return self.reverted_bid if self.reverted_bid > 0 else None


# In[ ]:


# @logger.catch
def main():
    global database
    database = database_controller.FB( database_controller.Database )
    performance_campaigns = database.get_running_campaign().to_dict('records')
    performance_campaigns = [campaign for campaign in performance_campaigns if (campaign['is_smart_bidding']=='True')]
    for campaign in performance_campaigns:
        days_passed = datetime.date.today() - campaign['ai_start_date']
        logger.info("Campaign Id: {}".format(campaign['campaign_id']), feature="f-strings")
        logger.info("Campaign Days Passed: {}".format(days_passed))
        if days_passed >= datetime.timedelta(5):
            logger.info("Calibration Condition Matched.")
            adset_ids = data_collector.Campaigns(campaign_id=campaign['campaign_id'],
                                                 database_fb=database).get_adsets_active()
            for adset_id in adset_ids:
                ins = Insights(campaign_id=campaign['campaign_id'], adset_id=adset_id)
                if ins.is_available:
                    bid = ins.polynomial_fit()
                    if bid:
                        database.update("adset_initial_bid", {'bid_amount': ins.reverted_bid.astype(object)}, adset_id=ins.adset_id)
                        database.insert(
                            table_name="adset_bidding_calibration_log",
                            values_dict={
                                "campaign_id": ins.campaign_id,
                                "adset_id": ins.adset_id,
                                "sequence": ins.sequence,
                                "result_bid": ins.reverted_bid,
                                "origin_bid": ins.origin_bid,
                                "request_time": datetime.datetime.today() - datetime.timedelta(1),
                            }
                        )
                logger.info("  AdSet Id: {}".format(adset_id))
                logger.info("    Is Available: {}".format(ins.is_available))
                logger.info("    Sequence: {}".format(ins.sequence))
                logger.info("    Result Bid: {}".format(bid))
                logger.info("    Origin Bid: {}".format(ins.origin_bid))
        else:
            logger.info("Calibration Condition not Matched.")


# In[ ]:


if __name__=='__main__':
    main()


# In[ ]:


# !jupyter nbconvert --to script facebook_auto_calibration.ipynb


# In[ ]:


# database = database_controller.FB( database_controller.Database )
# campaign_id = 23843468131980091
# adset_id = 23843886505160091
# ins = Insights(campaign_id=campaign_id, adset_id=adset_id)


# 0.317138,0.983337,0.974521,0.131263,0.965162,0.509806,0.196673
# awareness,interest,desire,action,landing_page_view,link_click,reach,impressions,ctr,spend,bid_amount
# 
# 5,4,0,0,NULL,NULL,1117,1387,NULL,245,848
# 
# 2,8,3,1,NULL,NULL,464,568,NULL,76,433

# weight = np.array([0.317138,0.983337,0.974521,0.131263,0.965162,0.509806,0.196673])
# a = np.array([5,4,0,0,np.nan,np.nan,1117,1387,np.nan,245,848])
# b = np.array([2,8,3,1,np.nan,np.nan,464,568,np.nan,76,433])
