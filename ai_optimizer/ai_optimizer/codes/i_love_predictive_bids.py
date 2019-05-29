#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql_adactivity_save
import pandas as pd
DATABASE = "dev_facebook_test"

BRANDING_LIST = ['LINK_CLICKS', 'ALL_CLICKS', 'VIDEO_VIEWS']

TIME_WINDOW_CONST = 36
PREDICT_STEP = 6


# In[3]:


def make_df_train(df_train):
    df_train = pd.concat([df_train.shift(-5),df_train.shift(-4),df_train.shift(-3),df_train.shift(-2),df_train.shift(-1),df_train], axis=1, sort=False)

    df_train = df_train.dropna().reset_index(drop=True)
    return df_train


# In[85]:


def make_predict():
#     %matplotlib inline
    df_camp = mysql_adactivity_save.get_campaign_target()
    df_branding = df_camp[df_camp['charge_type'].isin( BRANDING_LIST )]
    branding_campaign_id_list = df_branding.campaign_id.tolist()
    
    mydb = mysql_adactivity_save.connectDB(DATABASE)
    mycursor = mydb.cursor()
    for campaign_id in branding_campaign_id_list:
        insights_sql = "select campaign_id, bid_amount from campaign_insights where campaign_id={} order by request_time desc limit 1".format(campaign_id)
        df_insights = pd.read_sql( insights_sql , con=mydb )

        sql = "select cost_per_target from campaign_insights where campaign_id={} order by request_time desc limit {}".format(campaign_id,TIME_WINDOW_CONST)
        df = pd.read_sql( sql , con=mydb )
        df_train_y = df.head(PREDICT_STEP)
        df_train_y = make_df_train(df_train_y)
        df.drop(df.head(PREDICT_STEP).index, inplace=True)
        if len(df) < TIME_WINDOW_CONST and len(df) >= PREDICT_STEP:
            size = len(df.index)//PREDICT_STEP * PREDICT_STEP
            df_train_x = df.head(size)
            df_train_x = make_df_train(df_train_x)
            result = i_love_predict(df_train_x, df_train_y)
            df_insights['predict_bids'] = result
            mysql_adactivity_save.intoDB("campaign_predict_bids", df_insights)
    mydb.close()


# In[74]:


# %matplotlib inline
def i_love_predict(df_train_x, df_train_y):
    import matplotlib.pyplot as plt
    import numpy as np
    from sklearn import linear_model
    from sklearn.metrics import mean_squared_error, r2_score
    regr = linear_model.Ridge(alpha=0.1)
    regr.fit(df_train_x.iloc[0].as_matrix().reshape(-1,1), df_train_y.iloc[0].as_matrix().reshape(-1,1))
    df_y_pred = regr.predict(df_train_y['cost_per_target'].iloc[0].as_matrix().reshape(-1, 1))
    plt.plot([i for i in range(12)],
             df_train_x['cost_per_target'].iloc[0].as_matrix().reshape(1,-1)[0].tolist() + df_y_pred.reshape(1,-1)[0].tolist(),
             color='blue', linewidth=3)

    plt.xticks(())
    plt.yticks()

    plt.show()
    return str(df_y_pred.reshape(1,-1)[0].tolist())


# In[86]:


if __name__ == '__main__':
    make_predict()


# In[ ]:




