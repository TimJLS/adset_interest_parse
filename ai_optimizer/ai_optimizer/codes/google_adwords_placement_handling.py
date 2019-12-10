#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from googleads import adwords
import pandas as pd
import numpy as np
import datetime
import math
import gdn_datacollector as datacollector
import database_controller
import adgeek_permission as permission
import google_adwords_report_generator as report_generator
import google_adwords_controller as controller


# In[ ]:


def process_outlier_handling(database):
    performance_campaign_list = database.get_performance_campaign().to_dict('records')
    campaign_id_list = [campaign['campaign_id'] for campaign in performance_campaign_list]
    print('[processing]: campaign_id_list', campaign_id_list)
    for campaign in performance_campaign_list:
        campaign_id = campaign['campaign_id']
        placement_report_generator = report_generator.PlacementReportGenerator(campaign_id, media='gdn')
        placement_list = placement_report_generator.get_insights(date_preset=None)
        df_plcmnt = pd.DataFrame(placement_list)
        for x in placement_list:
            database.upsert("placement_insights", x)
        
        data_mean, data_std = np.mean(df_plcmnt.ctr), np.std(df_plcmnt.ctr)
        cut_off = data_std * 3
        lower, upper = data_mean - cut_off, data_mean + cut_off

        df_drop = df_plcmnt[df_plcmnt.ctr > upper]
        
        service_container_campaign = controller.CampaignServiceContainer(campaign['customer_id'])
        controller_campaign = controller.Campaign(service_container_campaign, campaign_id)
        resp = controller_campaign.negative_criterions.make_from_df(data=df_drop)
        for value in resp['value']:
            url = value['criterion']['url']
            database.upsert("placement_insights", {
                'campaign_id': campaign_id,
                'display_name': url,
                'status': 'excluded',
            })


# In[ ]:


def main():
    global database_gdn, database_gsn
    database_gdn = database_controller.GDN( database_controller.Database )
    database_gsn = database_controller.GSN( database_controller.Database )
    print('-'*20)
    print("[main]: processing gdn")
    process_outlier_handling(database=database_gdn)
    print('-'*20)
    print("[main]: processing gsn")
    process_outlier_handling(database=database_gsn)


# In[ ]:


if __name__=='__main__':
    main()


# In[ ]:


# !jupyter nbconvert --to script google_adwords_placement_handling.ipynb


# In[ ]:




