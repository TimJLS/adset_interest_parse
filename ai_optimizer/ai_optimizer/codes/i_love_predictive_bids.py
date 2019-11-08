#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import mysql_adactivity_save
import pandas as pd
import facebook_datacollector as datacollector
import database_controller
DATABASE = "dev_facebook_test"

BRANDING_LIST = ['LINK_CLICKS', 'ALL_CLICKS', 'VIDEO_VIEWS', 'REACH', 'IMPRESSIONS']

TIME_WINDOW_CONST = 36
PREDICT_STEP = 6


# In[ ]:


def make_df_train(df_train):
    df_train.pop('status')
    df_train_x = pd.concat(
        [df_train.shift(-5),
         df_train.shift(-4),
         df_train.shift(-3),
         df_train.shift(-2),
         df_train.shift(-1),
         df_train
        ], axis=1, sort=False)
    df_train_y = df_train.shift(-6)
    df_train_x = df_train_x.dropna().reset_index(drop=True)
    df_train_y = df_train_y[['cost_per_target']].dropna().reset_index(drop=True)
    return df_train_x, df_train_y


# In[ ]:


def make_predict():
#     %matplotlib inline
    global database_fb
    database_fb = database_controller.FB(database_controller.Database)
    branding_campaign_list = database_fb.get_branding_campaign().to_dict('records')
    
    print('[campaign_id_list]: ', [campaign.get('campaign_id') for campaign in branding_campaign_list])
    for campaign in branding_campaign_list:
        print('[campaign id]: ', campaign.get('campaign_id'))
        df_insights = database_fb.retrieve("campaign_insights", campaign_id=campaign.get('campaign_id')).tail(1)
        
        df = database_fb.retrieve("campaign_insights", campaign_id=campaign.get('campaign_id')).tail(TIME_WINDOW_CONST)
        df_train_x, df_train_y = make_df_train(df)
        df.drop(df.head(PREDICT_STEP).index, inplace=True)
        if len(df_train_x) < TIME_WINDOW_CONST and len(df_train_x) >= PREDICT_STEP:
            size = len(df_train_x.index)//PREDICT_STEP * PREDICT_STEP
            df_train_x, df_train_y = df_train_x.tail(size), df_train_y.tail(size)
            result = i_love_predict(df_train_x, df_train_y)
            
            df_insights['predict_bids'] = result
            for col in ['target', 'reach', 'spend', 'status', 'impressions', 'cost_per_target', 'request_time']:
                df_insights.pop(col)
            database_fb.upsert("campaign_predict_bids", df_insights.to_dict('records')[0])
        else:
            print('[make_predict]: campaign_id {} not enough data to predict.'.format(campaign.get('campaign_id')))


# In[ ]:


# %matplotlib inline
def i_love_predict(df_train_x, df_train_y):
    import matplotlib.pyplot as plt
    import numpy as np
    from sklearn import linear_model
    from sklearn.metrics import mean_squared_error, r2_score
    df_train_x.pop('request_time')#, df_train_y.pop('request_time')
    df_train_x.pop('campaign_id')#, df_train_y.pop('campaign_id')
    regr = linear_model.Ridge(alpha=0.0001)
#     regr = linear_model.LinearRegression()
    predict_bids_list = []
    regr.fit(df_train_x[['cost_per_target']].values, df_train_y.values)
    for i in range(0, 6):
        df_y_pred = regr.predict(df_train_x[['cost_per_target']].tail(6).iloc[[i]].values)
        train_x_list = df_train_x['cost_per_target'].tail(6).iloc[i].as_matrix().tolist()
        train_x_list.reverse()
        plt.plot(
            [i for i in range(7)],
            train_x_list + df_y_pred.reshape(1,-1)[0].tolist(),
            color='blue', linewidth=3)
        predict_bids_list.append( df_y_pred.reshape(1,-1)[0].tolist()[0] )
    plt.show()
    return str(predict_bids_list)


# In[ ]:


# %matplotlib inline
if __name__ == '__main__':
    make_predict()


# In[ ]:


# !jupyter nbconvert --to script i_love_predictive_bids.ipynb


# In[ ]:




