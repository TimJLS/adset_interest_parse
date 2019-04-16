#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gdn_datacollector
import datetime
import gdn_db
from googleads import adwords
AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
DATABASE = "dev_gdn"


# In[2]:


def get_sorted_adgroup(campaign):
    mydb = gdn_db.connectDB(DATABASE)
    try:
        df = pd.read_sql(
            "select * from adgroup_score where campaign_id=%s" % (campaign), con=mydb)
        df = df[df.request_time.dt.date == DATE].sort_values(
            by=['score'], ascending=False)
        adgroup_list = df['adgroup_id']
        assert adgroup_list, 'Empty List'
    except:
        df_camp = gdn_db.get_campaign(campaign)
        destination_type = df_camp['destination_type'].iloc[0]
        adgroup_list = gdn_datacollector.Campaign(
            df_camp['customer_id'][df_camp.campaign_id==campaign].iloc[0],
            campaign,
            destination_type).get_adgroup_id_list()
    return adgroup_list

def split_adgroup_list(adgroup_list):
    import math
    adgroup_list.sort(reverse=True)
    half = math.ceil(len(adgroup_list) / 2)
    return adgroup_list[:half], adgroup_list[half:]

def main():
    starttime = datetime.datetime.now()
    df_camp = gdn_db.get_campaign()
    campaign_id_list = df_camp['campaign_id'].unique()
    for campaign_id in campaign_id_list:
        client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)
        adgroup_list = get_sorted_adgroup(campaign_id)
        adgroup_for_copy, adgroup_for_off = split_adgroup_list(adgroup_list)
        for adgroup_id in adgroup_for_off:
            
            adgroup = gdn_datacollector.AdGroup( 
                df_camp['customer_id'].iloc[0],
                df_camp['campaign_id'].iloc[0],
                df_camp['destination_type'].iloc[0],
                adgroup_id )
            adgroup.update_status(client)
    return


# In[ ]:


if __name__=="__main__":
    main()


# In[ ]:




