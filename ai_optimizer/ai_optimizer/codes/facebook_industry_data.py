#!/usr/bin/env python
# coding: utf-8

# In[1]:




from pathlib import Path
import datetime
import adgeek_permission as permission
import pandas as pd
import database_controller

def search_target_keyword(keyword):
    from facebook_business.adobjects.targetingsearch import TargetingSearch
    params = {
        'q': str(keyword),
        'type': TargetingSearch.TargetingSearchTypes.interest,
    }
    search_target_result_list = TargetingSearch.search(params=params)
    return search_target_result_list


# In[2]:


a_id = 350498128813378
permission.init_facebook_api(a_id)

def lookup_interest_by_keyword(keyword):
    search_target_result_list = search_target_keyword(keyword)
    for search_target_result in search_target_result_list:
            target_id = search_target_result.get('id')
            target_name = search_target_result.get('name')
            target_audience_size = search_target_result.get('audience_size')
            if target_name == keyword:
                return target_id, target_name, target_audience_size
    return None, None, None
                


# In[3]:


target_id, target_name, target_audience_size = lookup_interest_by_keyword('籃球')
print(target_id, target_name, target_audience_size)


# In[ ]:





# In[ ]:


col_names =  ['industry', 'target_keyword', 'target_id', 'target_audience_size']

db = database_controller.Database()
database_fb = database_controller.FB(db)
industry_data = pd.read_csv('industry_data_raw.csv')

for index, row in industry_data.iterrows():
    industry = row['產業']
    target_keyword = row['條件受眾']
    target_id, target_name, target_audience_size = lookup_interest_by_keyword(target_keyword)
    print(industry, target_keyword, target_id, target_name, target_audience_size)
    if target_id:
        database_fb.upsert(
            "facebook_industry_suggestion",
            {
                'industry_type': industry,
                'target_keyword': target_keyword,
                'target_id': int(target_id),
                'target_audience_size': int(target_audience_size),
            }
        )
        


# In[ ]:




