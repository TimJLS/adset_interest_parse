
# coding: utf-8

from numpy.random import seed
seed(28)
from tensorflow import set_random_seed
set_random_seed(28)
# In[1]:

import numpy as np
import pandas as pd
#import seaborn as sns
#import matplotlib.pyplot as plt
import datetime
import calendar
import random
pd.set_option('display.max_columns', 500)

from sklearn.externals import joblib

from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout
from keras import optimizers, metrics
from keras.callbacks import EarlyStopping
from keras.models import load_model
from sklearn.externals import joblib 
from sklearn.preprocessing import StandardScaler
import tensorflow as tf
tf.device('/cpu:3')
BATCH_SIZE = 1
ITERATION = 50
EPOCH = 50
NEURONS = 64
MODEL_NAME = '/storage/opt_project/opt/models/cpc_48/cpc_predict_' +  str(ITERATION) + '_' + str(EPOCH) + '_' + str(NEURONS) + "_"  + '.h5'
DAILY_COUNT = 10
OPT_GOAL = 'LINK_CLICKS'
HOUR_PERIOD = 11
# In[2]:


ad_file = '/storage/opt_project/opt/data/act_1587964461298459_ad.csv'
adset_file = '/storage/opt_project/opt/data/act_1587964461298459_adset.csv'


# In[3]:




def readFiles(ad_file, adset_file):
    df_ad = pd.read_csv(ad_file,index_col=False, na_values = ['None'])
    df_adset = pd.read_csv(adset_file,index_col=False ,na_values = ['None'])
    df_ad = df_ad.rename(columns= {'hourly_stats_aggregated_by_audience_time_zone':'hour'})
    df_adset = df_adset.rename(columns= {'id':'adset_id'})
    
    df_merge = pd.merge(df_ad, df_adset, left_on='adset_id', right_on=['adset_id'], how='left')
    df_merge['date_conv'] = pd.to_datetime(df_merge['date_start'], format="%y-%m-%d", infer_datetime_format=True)
    df_merge['year'] = df_merge['date_conv'].dt.year
    df_merge['month'] = df_merge['date_conv'].dt.month
    df_merge['day'] = df_merge['date_conv'].dt.day
    
    weekday = []
    for i, wkd in enumerate(df_merge['date_conv']):
        weekday.append([calendar.day_name[df_merge['date_conv'][i].weekday()]])
    WeekDay = pd.DataFrame(data=weekday,columns=['WeekDay'])
    df_merge = pd.concat([df_merge,WeekDay],axis=1)
    
    df_merge = df_merge.drop(['name','bid_strategy'],axis=1)
    
    df_merge['cpm'] = pd.to_numeric(df_merge['cpm'], errors='coerce')
    df_merge['cpc'] = pd.to_numeric(df_merge['cpc'], errors='coerce')
    df_merge['cpm'].fillna(value=0,inplace=True)
    df_merge['cpc'].fillna(value=0,inplace=True)
    df_merge = df_merge.drop(['date_start'],axis=1)
    df_merge = df_merge.sort_values(by=['adset_id','ad_id','date_conv','hour']).reset_index(drop=True)
    
    df_goal_click = df_merge[df_merge.optimization_goal == OPT_GOAL]
    df_goal_click = df_goal_click.loc[df_goal_click.hour.isin(list(range(HOUR_PERIOD)))]
    
    ad_id_daily_count = df_goal_click.groupby(["ad_id","date_conv"]).size()

    daily_count_df = pd.DataFrame({'total_count' : df_goal_click.groupby(["ad_id","date_conv"]).size() }).reset_index()
    daily_count_df = daily_count_df[ daily_count_df.total_count > DAILY_COUNT ]

    df_goal_click = df_goal_click[ df_goal_click.ad_id.isin(daily_count_df.ad_id) & df_goal_click.date_conv.isin(daily_count_df.date_conv) ]
    df_goal_click = df_goal_click.sort_values(by=['ad_name','adset_name','month','day','hour']).reset_index(drop=True)
    
    ad_name = df_goal_click[['ad_name']]

    #features that need not to onehot 
    df_not_onehot = df_goal_click[['hour','cpm','cpc','clicks','impressions','reach','spend','bid_amount','daily_budget','budget_remaining']]

    #adset_name=pd.get_dummies(df['adset_name'])
    hour = pd.get_dummies(df_goal_click['hour'])
    #month = pd.get_dummies(df_goal_click['month'])
    #day = pd.get_dummies(df_goal_click['day'])
    WeekDay = pd.get_dummies(df_goal_click['WeekDay'])
    #train = pd.concat( [ad_name, df_not_onehot , WeekDay, hour,month, day] ,axis=1)
    train = pd.concat( [ad_name, df_not_onehot , WeekDay, hour] ,axis=1)
    
    train_ad_name = pd.DataFrame({'total_count' : train.groupby(["ad_name"]).size() }).reset_index()
    return train, train_ad_name

#train, train_ad_name = readFiles(ad_file, adset_file)


# In[4]:


#get rid of cpc == 0
#train = train[train.cpc != 0]


# In[5]:


def processTrainData(train, train_ad_name):
    ad_set_list = []
    ad_set_hour_list = []
    ad_set_trainY_list = []
    
    for i in range(len(train_ad_name)):
        ad_set_list.append( train.groupby(['ad_name']).get_group(  train_ad_name.ad_name.iloc[i] ) )

    for i in range(len(train_ad_name)):
        ad_set_hour_list.append(  ad_set_list[i].hour )
        
    for i in range(len(train_ad_name)):
        ad_set_trainY_list.append(  ad_set_list[i][['hour','cpm','cpc','clicks','impressions','reach','spend']])

    ad_set_trainX_list = []

    for i in range(len(train_ad_name)):
        ad_set_trainX_list.append(  ad_set_list[i].drop([ 'hour' ],axis=1) )
    
    ad_set_trainX_3day_list = []
    #hour means current time, right columns are previous data 
    #hour is 10 , right colums are 9 , 8 , 7 data, which 7 data is NaN

    for i in range(len(train_ad_name)):
        ad_set_trainX_3day_list.append( pd.concat([ad_set_hour_list[i], ad_set_trainX_list[i].shift(1), ad_set_trainX_list[i].shift(2), ad_set_trainX_list[i].shift(3)],axis=1) )

    train_x_3day = []

    for i in range(len(train_ad_name)):
        train_x_3day.append( ad_set_trainX_3day_list[i])
        train_x_3day[i] = train_x_3day[i][ train_x_3day[i].hour != 0]
        train_x_3day[i] = train_x_3day[i][ train_x_3day[i].hour != 1]
        train_x_3day[i] = train_x_3day[i][ train_x_3day[i].hour != 2]
        train_x_3day[i] = train_x_3day[i].drop(['hour'],axis=1)
        train_x_3day[i] = train_x_3day[i].drop(['ad_name'],axis=1)

    train_y = []
    for i in range(len(train_ad_name)):
        train_y.append( ad_set_trainY_list[i])
        train_y[i] = train_y[i][ train_y[i].hour != 0]
        train_y[i] = train_y[i][ train_y[i].hour != 1]
        train_y[i] = train_y[i][ train_y[i].hour != 2]
        train_y[i] = train_y[i].drop(['hour'], axis=1)

    train_X = train_x_3day[0]
    for i in range(1,len(train_ad_name) ):
        train_X = train_X.append(train_x_3day[i] , ignore_index=True)

    train_Y = train_y[0]
    for i in range(1,len(train_ad_name) ):
        train_Y = train_Y.append(train_y[i] , ignore_index=True)

    nanset = set( np.where(pd.isna( train_X) )[0] )
    train_X = train_X.drop(nanset)
    train_Y = train_Y.drop(nanset)
    return train_X, train_Y
    
#data_x , data_y = processTrainData(train, train_ad_name)


# In[7]:


'''
excludeList = ['spend', 'cpm','daily_budget', 'budget_remaining']
excludeList += ['Friday', 'Monday','Saturday','Sunday', 'Thursday', 'Tuesday', 'Wednesday']

print(excludeList)

for columnName in excludeList:
    if columnName in data_x.columns:
        data_x = data_x.drop([columnName],axis=1)
'''

# In[8]:


test_count = 32

def splitTrainTestData(data_x , data_y, testIndexStart):

    testIndexEnd = testIndexStart + test_count
    
    test_X, test_Y = data_x.iloc[ testIndexStart: testIndexEnd ,:], data_y.iloc[ testIndexStart:testIndexEnd ,:]

    train_X = data_x.drop(data_x.index[[testIndexStart,testIndexEnd]], inplace=True)
    train_Y = data_y.drop(data_y.index[[testIndexStart,testIndexEnd]], inplace=True)
    
    return data_x , data_y, test_X, test_Y 

#train_X , train_Y, test_X, test_Y  = splitTrainTestData(data_x , data_y, random.randint(test_count, len(data_x) - test_count))


# In[9]:


def scaleData(train_X , train_Y):
    
    train_X_arr = train_X.values
    train_Y_arr = train_Y[['clicks','cpc']].values
    #train_Y_arr = train_Y_arr.reshape(-1,1)

    scalerX = StandardScaler().fit(train_X_arr)
    scaled_X = scalerX.transform(train_X_arr)
    scalerY = StandardScaler().fit(train_Y_arr)
    scaled_Y = scalerY.transform(train_Y_arr)
    return scalerX, scaled_X, scalerY, scaled_Y

#scalerX, scaled_X, scalerY, scaled_Y = scaleData(train_X , train_Y )

'''
scalerX_filename = "scalerX.pkl"
scalerY_filename = "scalerY.pkl"
joblib.dump(scalerX, scalerX_filename) 
joblib.dump(scalerY, scalerY_filename) 
'''

# In[10]:


#Stateful 效果較好
#Dropout 效果較差
def create_model(train, target, batch_size, iteration, neurons, epoch):
    x, y = train, target
    x = train.reshape(train.shape[0], 1, train.shape[1])
    #x = train.values.reshape(train.shape[0], 1, train.shape[1])
    
    model = Sequential()
    model.add(LSTM( int(neurons) , batch_input_shape=(batch_size, x.shape[1], x.shape[2]), stateful = True, return_sequences=True))
    model.add(Dropout(0.2))

    model.add(LSTM(neurons, batch_input_shape=(batch_size, x.shape[1], x.shape[2]), stateful=True))
    model.add(Dropout(0.2))
    model.add(Dense(target.shape[1]))
    model.compile(loss='mean_squared_error', optimizer='adam',metrics=[metrics.mse])
    
    es = EarlyStopping(monitor='val_loss', patience=5)
    for i in range(iteration):
        print('====round', str(i) +  '/' + str(epoch))
        history = model.fit(x, y, epochs=EPOCH, batch_size=batch_size,
                  verbose=1, shuffle=False, validation_split=0.2, callbacks=[es])
        #plt.plot(history.history['val_loss'], 'r')
        model.reset_states()
    model.save(MODEL_NAME)
    return model
#create_model(scaled_X,scaled_Y, BATCH_SIZE, ITERATION, NEURONS, EPOCH)


# In[11]:


def evaluateModel(test_X):
    #load model and scaler
    model = load_model(MODEL_NAME)    
    scalerX = joblib.load(scalerX_filename) 
    scalerY = joblib.load(scalerY_filename)     
    
    test_X = scalerX.transform(test_X)

    test_X = test_X.reshape(test_X.shape[0],1,test_X.shape[1])
    pred_click, pred_cpc = [], []
    for i,arr in enumerate(test_X):
        ans = model.predict(test_X[i:i+1,:,:])
        ans = scalerY.inverse_transform(ans)
        pred_click.append(ans[0,0])
        pred_cpc.append(ans[0,1])
    pred = pd.DataFrame({'pred_click':pred_click,'pred_cpc':pred_cpc})
    ans = pd.DataFrame(data=ans)
    frm = [pred,test_Y.reset_index(drop=True)]
    pred = pd.concat(frm,axis=1,sort=False)
    
    pred['delta_click'] = pred['pred_click']-pred['clicks']
    pred['delta_cpc'] = pred['pred_cpc']-pred['cpc']
    return pred

#pred = evaluateModel(test_X)
#print(pred.columns)
#pred[['pred_click', 'clicks', 'delta_click' , 'pred_cpc' , 'cpc', 'delta_cpc', 'impressions']]
def main():
    train, train_ad_name = readFiles(ad_file, adset_file)
    train = train[train.cpc != 0]
    data_x , data_y = processTrainData(train, train_ad_name)
    excludeList = ['spend', 'cpm','daily_budget', 'budget_remaining']
    excludeList += ['Friday', 'Monday','Saturday','Sunday', 'Thursday', 'Tuesday', 'Wednesday']

    print(excludeList)

    for columnName in excludeList:
        if columnName in data_x.columns:
            data_x = data_x.drop([columnName],axis=1)
    train_X , train_Y, test_X, test_Y  = splitTrainTestData(data_x, data_y, random.randint(test_count, len(data_x) - test_count))
    scalerX, scaled_X, scalerY, scaled_Y = scaleData(train_X , train_Y )
    
    scalerX_filename = "/storage/opt_project/opt/models/cpc_48/scalerX_exclude_zero.pkl"
    scalerY_filename = "/storage/opt_project/opt/models/cpc_48/scalerY_exclude_zero.pkl"
    joblib.dump(scalerX, scalerX_filename) 
    joblib.dump(scalerY, scalerY_filename)
    
    create_model(scaled_X,scaled_Y, BATCH_SIZE, ITERATION, NEURONS, EPOCH)
    
if __name__ == '__main__':
    main()