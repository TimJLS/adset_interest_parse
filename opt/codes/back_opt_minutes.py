
# coding: utf-8

# In[10]:


#from numpy.random import seed
#seed(1)
#from tensorflow import set_random_seed
#set_random_seed(1)
import numpy as np
#from keras.models import Sequential
import pandas as pd
#import matplotlib.pyplot as plt
import calendar
import datetime
import os.path
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.externals import joblib
import tensorflow as tf
#from keras.models import load_model


# In[2]:


CAMPAIGN_DAY = 10# 廣告走期
CAMPAIGN_BUDGET = 10000#廣告總預算
CAMPAIGN_CLICK = 100#廣告KPI
HOURS_DAY = 10#單日時長
day_click = CAMPAIGN_CLICK/CAMPAIGN_DAY#單日KPI
day_budget = CAMPAIGN_BUDGET/CAMPAIGN_DAY#單日預算~~~~~~~~~~~~~待修改
#process = days/CAMPAIGN_DAY#campaign進度
hour_speed = 100#單小時走速
hour_click = day_click/HOURS_DAY#單小時KPI
hour_budget = day_budget/HOURS_DAY#單小時預算
col_names = ['ad_id','cpc','clicks','impressions','reach','bid_amount','0','1','2','3','4','5','6',
             '7','8','9','10']
data_path = Path('/storage/opt_project/opt/data/real_time_ad_data.csv')
pred_data_path = Path('/storage/opt_project/opt/data/pred_ad_data.csv')


# In[3]:

import numpy as np
import pandas as pd
from sklearn.externals import joblib
from keras.models import load_model, model_from_json

FOLDER_PATH = '/storage/opt_project/opt/models/cpc_48/'
MODEL_PATH = FOLDER_PATH + 'cpc_predict_20_20_32_.h5'
print('MODEL_PATH:',MODEL_PATH )

model = ''
graph = ''
is_load_model = False
model = load_model(MODEL_PATH)

SCALER_X_PATH = FOLDER_PATH + 'scalerX.pkl'
SCALER_Y_PATH = FOLDER_PATH + 'scalerY.pkl'
scalerX = joblib.load(SCALER_X_PATH)
scalerY = joblib.load(SCALER_Y_PATH)


# In[31]:


def decide_strategy(ad_id,cpc,clicks,impressions,reach,bid_amount):
    request_time = datetime.datetime.now()#.strftime("%Y-%m-%d %H:%M:%S")
    print('request sent : ',request_time)
    day = request_time.day
    hour = request_time.hour
    timetable = get_time(hour)#當前時間表
    d={'ad_id':[ad_id],'request_time':[request_time],'cpc':[cpc],'clicks':[clicks],'impressions':[impressions],'reach':[reach],'bid_amount':[bid_amount]}
    alltime = save(d,timetable)
    if check_seq(ad_id,day,hour):
        time_seq = time_seq_make(ad_id,day,hour)
        print('Predicting')
        cpc, budget = decide_setting(ad_id,hour,time_seq)
        print('Prediction complete')
        return int(cpc),int(budget)
    else:
        print('settings no change.')
        return int(cpc), int(day_budget)


# In[15]:


def time_seq_make(ad_id,day,hour):
    df = pd.read_csv(data_path)
    seq = df[df.ad_id == ad_id]
    seq['request_time'] = pd.to_datetime(seq['request_time'], format="%Y-%m-%d %H:%M:%S.%f")
    seq = seq[seq['cpc']!=0]
    seq=seq[seq['request_time'].dt.day==day]
    seq=seq[seq['request_time'].dt.hour!=hour]
    seq=seq.tail(180)
    seq['hour'] = seq['request_time'].dt.hour
    s = seq.hour.unique()
    #print('len(s):',len(s))
    if len(s) < 3:
        print("ad_id ",ad_id," data collected")
        return None
    else:
        s1_tail, s1_head = seq[seq.hour == s[-1]].drop(['ad_id','hour','request_time'],axis=1).tail(1), seq[seq.hour == s[-1]].drop(['ad_id','hour','request_time'],axis=1).head(1)
        s1_tail, s1_head = s1_tail.reset_index(drop=True), s1_head.reset_index(drop=True)
        s1 = s1_tail.sub(s1_head,axis=0)
        s1['cpc']=compute_cpc(s1_tail['cpc'].iloc[0], s1_tail['clicks'].iloc[0], s1_head['cpc'].iloc[0], s1_head['clicks'].iloc[0])
        
        s2_tail, s2_head = seq[seq.hour == s[-2]].drop(['ad_id','hour','request_time'],axis=1).tail(1), seq[seq.hour == s[-2]].drop(['ad_id','hour','request_time'],axis=1).head(1)
        s2_tail, s2_head = s2_tail.reset_index(drop=True), s2_head.reset_index(drop=True)
        s2 = s2_tail.sub(s2_head,axis=0)
        s2['cpc']=compute_cpc(s2_tail['cpc'].iloc[0], s2_tail['clicks'].iloc[0], s2_head['cpc'].iloc[0], s2_head['clicks'].iloc[0])
        
        s3_tail, s3_head = seq[seq.hour == s[-3]].drop(['ad_id','hour','request_time'],axis=1).tail(1), seq[seq.hour == s[-3]].drop(['ad_id','hour','request_time'],axis=1).head(1)
        s3_tail, s3_head = s3_tail.reset_index(drop=True), s3_head.reset_index(drop=True)
        s3 = s3_tail.sub(s3_head,axis=0)
        s3['cpc']=compute_cpc(s3_tail['cpc'].iloc[0], s3_tail['clicks'].iloc[0], s3_head['cpc'].iloc[0], s3_head['clicks'].iloc[0])
        
        time_seq = pd.concat([s1,s2,s3],axis=1,sort=False)
        return time_seq


# In[16]:


def compute_cpc(t_cpc, t_click, h_cpc, h_click):
    if t_click == h_click:
        return t_cpc
    else:
        return (t_cpc*t_click - h_cpc*h_click)/(t_click - h_click)     


# In[17]:


def check_seq(ad_id,day,hour):
    print('[check seq]')
    df = pd.read_csv(data_path)
    s = df[df.ad_id == ad_id]
    s['request_time'] = pd.to_datetime(s['request_time'], format="%Y-%m-%d %H:%M:%S.%f")
    s = s[s['cpc']!=0]
    s=s[s['request_time'].dt.day==day]
    s=s.tail(180)
    s['hour'] = s['request_time'].dt.hour
    s=s[s.hour!=hour]
    if len(s.hour.unique()) < 3:
        print('time seq not match')
        return False
    else:
        print('time seq match')
        return True


# In[18]:


def save(d,timetable):
    checkFile(col_names,data_path)
    ad_data = pd.DataFrame(data=d)
    hour_data = ad_data.join(timetable, how='outer')
    df = pd.read_csv(data_path)
    df = df.append(hour_data, ignore_index = True)
    #print(df)
    df.to_csv(data_path , index = False)
    df = df.drop(['request_time'],axis=1)
    return df


# In[19]:


def checkFile(col_names,data_path):
    print("[checkFile]")
    if not data_path.exists():
        df = pd.DataFrame(columns = col_names)
        df.to_csv(data_path , index=False)


# In[20]:


def decide_setting(ad_id,hour,time_seq):
    df = pd.read_csv(data_path)
    group = df.groupby('ad_id').get_group(ad_id)
    totalhourclick = group['cpc'].sum()
    #print("clicks to go :",totalhourclick)
    #print(alltime.iloc[-1,2:])#clicks
    #hour_budget_left = hour_budget-hourdata['spend']#每小時預算剩餘   
    #day_budget_left = day_budget-hourdata['spend'].sum()#單日目前預算剩餘
    s = speed(totalhourclick)#計算走速
    #判斷走速
    if s > hour_speed:
        print('KPI Match')
        next_cpc, next_budget = cpc_optimize(time_seq)
        print('cpc and budget', next_cpc , next_budget)
        print('---------')
    else:
        print('KPI NOT Match')
        next_cpc, next_budget = KPI_optimize(time_seq, hour, totalhourclick)
        print('cpc and budget', next_cpc , next_budget)
        print('---------')
    save_predict(ad_id,next_cpc)
    return int(next_cpc),int(next_budget)


# In[21]:


#預測下個時刻 cpc, clicks
def value_predict(time_seq):
    print('[value predict]')
    global model
    time_seq = time_seq.values
    time_seq = time_seq.reshape((1,1,time_seq.shape[1]))
    scaled_seq = scale(scalerX, time_seq)
    scaled_seq = scaled_seq.reshape((1,1,48))
    md = model.summary()
    y = model.predict(scaled_seq)
    y = inverse_scale(scalerY, y)
    #print("y[0][0]:",y[0][0]," y[0][1]:",y[0][1])
    return y[0][0], y[0][1]

def loading_model(MODEL_PATH):
    global model, graph
    model = tf.keras.models.load_model('/storage/opt_project/opt/models/cpc_predict_20_20_32_.h5')
    graph = tf.get_default_graph()
def predict_v(time_seq):
    global is_load_model
    if is_load_model is False:
        loading_model(MODEL_PATH)
        is_load_model = True
    print('[value predict]')
    global model, graph
    time_seq = time_seq.values
    time_seq = time_seq.reshape((1,1,time_seq.shape[1]))
    scaled_seq = scale(scalerX, time_seq)
    scaled_seq = scaled_seq.reshape((1,1,48))
    md = model.summary()
    y = model.predict(scaled_seq)
    y = inverse_scale(scalerY, y)
    #print("y[0][0]:",y[0][0]," y[0][1]:",y[0][1])
    return y[0][0], y[0][1]

# In[22]:


def save_predict(ad_id,next_cpc):
    request_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('[saving prediction]')
    col_names=['ad_id','request_time','pred_cpc']
    checkFile(col_names,pred_data_path)
    d={'ad_id':[ad_id],'request_time':[request_time],'pred_cpc':[next_cpc]}
    df = pd.read_csv(pred_data_path)
    df2 = pd.DataFrame(data=d)
    df = df.append(df2, ignore_index=True)
    #df = pd.concat([df,df2],axis=0)
    #print(df)
    df.to_csv(pred_data_path , index = False)
    return df


# In[23]:


def cpc_optimize(time_seq):
    pred_cpc, _ = value_predict(time_seq)
    print("pred_cpc:",pred_cpc)
    next_cpc = (  pred_cpc + time_seq['cpc'].iloc[0,0] ).mean()
    print("next_cpc:", next_cpc) 
    return int(next_cpc),int(day_budget)


# In[24]:


#應用 model 優化 "客戶KPI"
def KPI_optimize(time_seq, hour, sum_hour_click):
    pred_cpc, _ = value_predict(time_seq)
    print("pred_cpc:",pred_cpc)
    next_cpc = max( pred_cpc, time_seq['cpc'].iloc[0,0] )
    print("next_cpc:",next_cpc)
    budget =( hour_click * (hour+1) - sum_hour_click ) * next_cpc
    #campaign 即將結束, 增加cpc, 增加budget
    return int(next_cpc),int(day_budget+budget)
'''    if process > 0.8:
        cpc, budget = predict(time_seq)['cpc']+1, predict(time_seq)['budget']+target_budget_left
    #campaign 尚有時間, 固定cpc, 增加budget
    else:
        cpc, budget = hourdata['cpc'], predict(time_seq)['budget']+target_budget_left
'''


# In[25]:


def get_time(hour):
    #year = datetime.datetime.now().year
    #month = datetime.datetime.now().month
    #day = datetime.datetime.now().day
    #wkd = calendar.weekday(year,month,day)
    #weekday = list(calendar.day_name)
    #wkd_arr = np.zeros((1,7), dtype=int)
    #wkd_arr[0][wkd]=1
    #week=pd.DataFrame(data=wkd_arr,columns=weekday)
    #week = pd.concat([week],axis=1)
    hour_ = np.zeros((1,11), dtype=int)
    if hour < 11:
        hour_[0][hour-1]=1
    else:
        hour_[0][10]=1
    hour_axis = ['0','1','2','3','4','5','6',
             '7','8','9','10']
    hour_ = pd.DataFrame(data=hour_,columns=hour_axis)
    #result = week.join(hour_, how='outer')
    #print(result.columns)
    return hour_


# In[26]:


def scale(scalerX, x):
    x = x.reshape(1, 48)
    scaled_x = scalerX.transform(x)
    return scaled_x


# In[27]:


def inverse_scale(scalerY, y):
    y = np.reshape(y, (1, 2))
    inverse_y = scalerY.inverse_transform(y)
    return inverse_y


# In[28]:


#計算走速(clicks/12hours)
def speed(click):
    speed = click/HOURS_DAY
    print("speed : ",speed)
    print("hour speed : ",hour_speed)
    return speed


# In[30]:


if __name__ == '__main__':
    decide_strategy(28527413,0.00,4,69,68,9.0)


# In[ ]:


def main():
    decide_strategy(28527413,0.00,4,69,68,9.0)


