#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import copy
import math
import datetime
import pandas as pd
from loguru import logger
import adgeek_permission as permission
import google_adwords_controller as controller
import google_adwords_report_generator as report_generator
import database_controller

LOGGER_FOLDER = '/home/tim_su/ai_optimizer/opt/ai_optimizer/app_log/creative_controller/'


# In[ ]:


def process(database):
    PATH = LOGGER_FOLDER + '{media}/{date}.log'.format(
        media=database.media,
        date=datetime.datetime.strftime(datetime.datetime.today(),"%m_%d_%Y"))
    logger.add(PATH)
    
    performance_campaigns = database.get_performance_campaign().to_dict('records')
    performance_campaigns = [campaign for campaign in performance_campaigns
                             if eval(campaign['is_creative_opt'])]
    
    for campaign in performance_campaigns:
        campaign_id = campaign['campaign_id']
        kpi = campaign['ai_spend_cap'] / campaign['destination']
        rpg = report_generator.AdReportGenerator(campaign_id,
                                                 media=database.media)
        ads = rpg.get_insights(date_preset='entire_time')
        df_ads = pd.DataFrame(ads)
        conditions = ((df_ads.conversions == 0) & (df_ads.spend > kpi))
        df_ads = df_ads[conditions]
        
        service_container = controller.AdGroupServiceContainer(campaign['customer_id'])
        controller_campaign = controller.Campaign(service_container, campaign_id)
        
        creatives = [controller.Creative(controller_campaign.service_container.service_ad,
                                         ad['adgroup_id'],
                                         ad['ad_id'])
                     for ad in df_ads.to_dict('records')]
        for creative in creatives:
            creative.update(status=controller.Status.pause)


# In[ ]:


def main():
    db_host = database_controller.Database
    for database in [database_controller.GDN(db_host), database_controller.GSN(db_host)]:
        process(database)


# In[ ]:


if __name__ == '__main__':
    main()


# In[ ]:


# !jupyter nbconvert --to script google_adwords_creative_opt.ipynb


# In[ ]:




