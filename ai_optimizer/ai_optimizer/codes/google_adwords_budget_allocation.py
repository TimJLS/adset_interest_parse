#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import datetime

import json
import pandas as pd
from loguru import logger
import database_controller
import google_adwords_controller as controller
import google_adwords_report_generator as collector


# In[ ]:


class CampaignGroup:
    def __init__(self, campaign_group_id: int = None, data: pd.DataFrame = None, *arg, **kwarg):
        self.data = data
        if data is None:
            self.data = pd.DataFrame()
        if isinstance(data, pd.DataFrame):
            self._data_parsing()
            self.campaigns = [Campaign(campaign_id=row['campaign_id'],
                                       media=kwarg['media'],
                                       data=dict(row)) for _, row in self.data.iterrows()]

    def __repr__(self) -> str:
        return "{0}{1}".format(self.__class__, json.dumps(self.data.to_dict('records'), indent=4, default=str))

    def _data_parsing(self):
        assert len(self.data['campaign_group_id'].unique()) == 1, "Multiple Campaign Group Ids" +                                                                  ", should be only one Id."
        self.campaign_group_id = self.data['campaign_group_id'].unique()[0]
        self.budget = self.data['budget'].agg('sum').astype(object)

    def budget_allocation(self):
        self.campaigns = [campaign for campaign in self.campaigns if campaign.spend_ratio > 0.5]
        self.available_balance = sum([campaign.budget for campaign in self.campaigns])
        self.expected_destination = sum([campaign.expected_destination for campaign in self.campaigns])
        for campaign in self.campaigns:
            campaign.available_balance = (self.available_balance / self.expected_destination) *                                           campaign.expected_destination


# In[ ]:


class Campaign(collector.CampaignReportGenerator):
    _COST_OBJECTIVE = {
        'LINK_CLICKS': 'cost_per_click',
        'COVERSIONS': 'cost_per_conversion'
    }
    _OBJECTIVE = {
        'LINK_CLICKS': 'clicks',
        'COVERSIONS': 'conversions'
    }
    def __init__(self, campaign_id: int, media: str, data: dict = None, *arg, **kwarg):
        super().__init__(campaign_id, media)
        self.data = data
        self.objective = self._COST_OBJECTIVE[self.brief['destination_type']]
        self.insights = self.get_insights(date_preset='yesterday')[0]
        self.budget = self.data['budget'] if isinstance(data, dict) else None
        self.closing_price = self.insights[self._COST_OBJECTIVE[self.brief['destination_type']]]
        self.expected_destination = self.insights['daily_budget'] / self.closing_price if self.closing_price != 0 else 10
        self.actual_destination = self.insights[self._OBJECTIVE[self.brief['destination_type']]]
        self.spend_ratio = self.insights['spend'] / self.insights['daily_budget']
        self.service = controller.CampaignServiceContainer(self.customer_id)
        
    def __repr__(self) -> str:
        return "{0}{1}".format(self.__class__, json.dumps(self.insights, indent=4, default=str))


# In[ ]:


@logger.catch
def process_budget_allocation(database):
    PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/budget_allocation/' +            '{media}/{date}.log'.format(media=database.media,
                                       date=datetime.datetime.strftime(datetime.datetime.today(),
                                                                       "%m_%d_%Y"))
    logger.add(PATH)
    campaign_groups = database.get_running_campaign_group().groupby('campaign_group_id')
    
    if campaign_groups.size().empty:
        logger.info("No Campaign Groups to optimize.")
        return
    
    for _, campaign_group in campaign_groups:
        cgp = CampaignGroup(media=database.media, data=campaign_group)
        logger.debug("{}".format(cgp.campaigns))
        cgp.budget_allocation()
        logger.info("Campaign Group ID: {}".format(cgp.campaign_group_id))
        logger.info("    Budget: {}".format(cgp.budget))
        logger.info("    Available Balance: {}".format(cgp.available_balance))
        logger.info("    Expected Destinations: {}".format(cgp.expected_destination))
        if not cgp.campaigns:
            logger.info("Allocation Requirements not match.")
            return
        for campaign in cgp.campaigns:
            logger.info("Campaign ID: {}".format(campaign.campaign_id))
            logger.info("    Objective: {}".format(campaign.objective))
            logger.info("    Budget: {}".format(campaign.budget))
            logger.info("    Closing Price(CPC): {}".format(campaign.closing_price))
            logger.info("    Spend Ratio: {}".format(campaign.spend_ratio))
            logger.info("    Expected Destinations: {}".format(campaign.expected_destination))
            logger.info("    Actual Destinations: {}".format(campaign.actual_destination))
            logger.info("    Available Balance: {}".format(campaign.available_balance))
            controller_campaign = controller.Campaign(campaign.service, campaign.campaign_id)
            resp = controller_campaign.get_budget().update(campaign.available_balance)
            logger.info("    Update Response: {}".format(resp))


# In[ ]:


def main():
    db_host = database_controller.Database
    for database in [database_controller.GDN(db_host), database_controller.GSN(db_host)]:
        process_budget_allocation(database)


# In[ ]:


if __name__ == '__main__':
    main()


# In[ ]:


# !jupyter nbconvert --to script google_adwords_budget_allocation.ipynb

