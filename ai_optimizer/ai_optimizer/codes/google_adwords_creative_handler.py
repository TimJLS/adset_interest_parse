#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from googleads import adwords
import pandas as pd
import copy
import math
import datetime

import adgeek_permission as permission
import google_adwords_controller as controller
import database_controller

LIMMITED_STATUS_LIST = ['UNDER_REVIEW', 'DISAPPROVED']


# In[ ]:


def is_all_disapproved(controller_campaign):
    controller_campaign.get_ads()
    disapproved_creative_list = [
        creative.policy.approval_status for creative in controller_campaign.creatives if creative.policy.approval_status in LIMMITED_STATUS_LIST ]
    return len(controller_campaign.creatives) == len(disapproved_creative_list)


# In[ ]:


def handle_gdn_campaign():
    db = database_controller.Database()
    database_gdn = database_controller.GDN(db)
    running_campaign_dict_list = database_gdn.get_running_campaign().to_dict('records')
    campaign_id_list = [ running_campaign_dict['campaign_id'] for running_campaign_dict in running_campaign_dict_list ]
    print('[handle_gdn_campaign]: campaign_id_list', campaign_id_list)
    for running_campaign_dict in running_campaign_dict_list:
        customer_id = running_campaign_dict['customer_id']
        campaign_id = running_campaign_dict['campaign_id']
        service_container = controller.AdGroupServiceContainer( customer_id )
        controller_campaign = controller.Campaign(service_container, campaign_id)

        if is_all_disapproved(controller_campaign):
            print('====[handle_gdn_campaign]: all campaign creatives is disapproved')
            print('========[campaign_id]:', campaign_id)
            # stops intervention
            database_gdn.upsert("campaign_target", {"campaign_id": campaign_id, "ai_status": "inactive"})


# In[ ]:


def handle_gsn_campaign():
    db = database_controller.Database()
    database_gsn = database_controller.GSN(db)
    running_campaign_dict_list = database_gsn.get_running_campaign().to_dict('records')
    campaign_id_list = [ running_campaign_dict['campaign_id'] for running_campaign_dict in running_campaign_dict_list ]
    print('[handle_gsn_campaign]: campaign_id_list', campaign_id_list)
    for running_campaign_dict in running_campaign_dict_list:
        customer_id = running_campaign_dict['customer_id']
        campaign_id = running_campaign_dict['campaign_id']
        service_container = controller.AdGroupServiceContainer( customer_id )
        controller_campaign = controller.Campaign(service_container, campaign_id)

        if is_all_disapproved(controller_campaign):
            print('====[handle_gsn_campaign]: all campaign creatives is disapproved')
            print('========[campaign_id]:', campaign_id)
            # stops intervention
            database_gsn.upsert("campaign_target", {"campaign_id": campaign_id, "ai_status": "inactive"})


# In[ ]:


def main():
    start_time = datetime.datetime.now()
    print('current time: ', start_time)
    global database_gdn
    global database_gsn
    handle_gdn_campaign()
    handle_gsn_campaign()


# In[ ]:


if __name__=='__main__':
    main()


# In[ ]:


# !jupyter nbconvert --to script google_adwords_creative_handler.ipynb


# In[ ]:




