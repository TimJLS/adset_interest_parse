
# coding: utf-8

#from numpy.random import seed
#seed(1)
#from tensorflow import set_random_seed
#set_random_seed(1)
import numpy as np
import pandas as pd
import calendar
import datetime
import os.path
from pathlib import Path

from sklearn.preprocessing import StandardScaler
from sklearn.externals import joblib

import tensorflow as tf
from keras import backend as K
from keras.models import load_model, model_from_json

import mysql_adactivity_save
import fb_graph
HOUR_PER_DAY = 24#單日時長
PREDICT_HOUR_INTERVAL = 3
PREDICT_MINUTES_INTERVAL = PREDICT_HOUR_INTERVAL*180
REVIVE_HOUR = 10

HOUR_AXIS = ["0", '1', '2', '3', '4', '5', '6', '7', '8', '9', '10','11','12', '13', '14', '15', '16', '17', '18', '19', '20', '21','22','23']

FOLDER_PATH = '/storage/opt_project_test/optProjectTest/optProjectTest/models/cpc_90/'
MODEL_PATH = FOLDER_PATH + 'IT_stateful_alldata_20_500_128.h5'
MODEL_SHAPE = 90
OUTPUT_SHAPE = 2
COL_REQUEST_TIME = 'request_time'
COL_CLICKS = 'clicks'
COL_AD_ID = 'ad_id'
COL_HOUR = 'hour'
COL_CPC = 'cpc'
COL_DATE = 'date'
COL_IMPRESS = 'impressions'
COL_REACH = 'reach'
COL_BID_AMOUNT = 'bid_amount'

SCALER_X_PATH = FOLDER_PATH + 'IT_stateful_scalerX.pkl'
SCALER_Y_PATH = FOLDER_PATH + 'IT_stateful_scalerY.pkl'
scalerX = joblib.load(SCALER_X_PATH)
scalerY = joblib.load(SCALER_Y_PATH)

# class RnnModel:
model = load_model(MODEL_PATH)
graph = tf.get_default_graph()

# In[31]:


def decide_strategy(ad_id, cpc, clicks, impressions, reach, bid_amount, daily_budget):
    request_time = datetime.datetime.now()#.strftime("%Y-%m-%d %H:%M:%S")
    day = request_time.day
    hour = request_time.hour
    timetable = get_time( hour )#當前時間表
    d={ 'ad_id':[ad_id], 'request_time':[request_time], 'cpc':[cpc], 'clicks':[clicks],
       'impressions':[impressions], 'reach':[reach], 'bid_amount':[bid_amount], 'daily_budget':[daily_budget] }
    save( d, timetable )
    time_seq = time_seq_make( ad_id, day, hour )
    total_clicks = mysql_adactivity_save.get_clicks_data( ad_id )
    campaign_id = fb_graph.Ads(ad_id).get_camp_id()
    s = compute_speed( ad_id )
    avgspeed,_,_ = mysql_adactivity_save.get_speed(ad_id)
    
    if time_seq.empty:
        print('settings no change.')
        decide_type = 'Data Collect'
        reasons = 'settings no change.'
        if hour >= REVIVE_HOUR:
            decide_type = 'Revive'
            reasons = 'Low performance'
            cpc = fb_graph.revive_bidding(ad_id)
            daily_budget = mysql_adactivity_save.get_init_budget(ad_id)

            save_revive(ad_id, cpc, daily_budget, decide_type, s, avgspeed)
        return int(cpc), int(daily_budget), decide_type, reasons
    else:
        if s > avgspeed:
            print('DO CPC OPT')
            cpc, budget, pred_cpc, pred_clicks = cpc_optimize( ad_id, time_seq )
            decide_type = 'CPC'
            reasons = 'KPI reached, start CPC optimize'
        else:
            print('DO KPI OPT')
            totalhourclick = time_seq[COL_CLICKS].iloc[0]
            cpc, budget, pred_cpc, pred_clicks = KPI_optimize( ad_id, time_seq, hour, totalhourclick, daily_budget )
            decide_type = 'KPI'
            reasons = "KPI haven't reached yet, start KPI optimize"
        print('Prediction complete')
        if cpc > 12:
            cpc = 12
        elif budget > 10000:
            budget = 10000
        save_predict( ad_id, cpc, budget, pred_cpc, pred_clicks, s, avgspeed, decide_type )

        return int(cpc), int(budget), decide_type, reasons


def time_seq_make(ad_id, day, hour):
    seq = mysql_adactivity_save.getAllTimeData( ad_id )
    seq[COL_REQUEST_TIME] = pd.to_datetime( seq[COL_REQUEST_TIME], format="%Y-%m-%d %H:%M:%S.%f" )
    seq = seq[ seq[COL_CPC] != 0 ]
    seq = seq[ ( seq[COL_REQUEST_TIME].dt.day == day ) & ( seq[COL_REQUEST_TIME].dt.hour != hour ) ]
    seq = seq.tail( PREDICT_MINUTES_INTERVAL )
    seq[COL_HOUR] = seq[COL_REQUEST_TIME].dt.hour
    s = seq.hour.unique()
#     print('+'*30)
#     print('len(s):',len(s))
    if len(s) < PREDICT_HOUR_INTERVAL:
#         print('time_seq not match')
#         print("ad_id ", ad_id, " data collected")
        time_seq = pd.Series()
        return time_seq
    else:
#         print('time_seq match')
        time_seq = concat_seq( seq, s )
        return time_seq


def concat_seq(seq, s):
    s = s[::-1] # reverse seq
    dfs=[]
    for i in range(0, PREDICT_HOUR_INTERVAL):
        tail = seq[ seq.hour == s[i] ].drop( [ COL_AD_ID, COL_HOUR, COL_REQUEST_TIME ], axis=1 ).tail(1).squeeze()
        head = seq[ seq.hour == s[i] ].drop( [ COL_AD_ID, COL_HOUR, COL_REQUEST_TIME ], axis=1 ).head(1).squeeze()
        
        tail_cpc, tail_clicks = tail[COL_CPC], tail[COL_CLICKS]
        head_cpc, head_clicks = head[COL_CPC], head[COL_CLICKS]
        
        tail_sub = tail.loc[[COL_CPC, COL_CLICKS, COL_IMPRESS, COL_REACH]]
        head_sub = head.loc[[COL_CPC, COL_CLICKS, COL_IMPRESS, COL_REACH]]
        
        seq_tmp = tail_sub.sub( head_sub, axis='index' )
        seq_tmp = seq_tmp.append( tail.loc[[ COL_BID_AMOUNT ]] )
        seq_tmp = seq_tmp.append( tail.loc[ HOUR_AXIS ] )
        seq_tmp[COL_CPC] = compute_cpc( tail_cpc, tail_clicks, head_cpc, head_clicks )
        dfs.append(seq_tmp)
    time_seq = pd.concat( dfs, axis=0, sort=False )
    return time_seq
# In[16]:


def compute_cpc(t_cpc, t_click, h_cpc, h_click):
    if t_click == h_click:
        return t_cpc
    else:
        return ( t_cpc*t_click - h_cpc*h_click ) / ( t_click - h_click )     


# In[18]:


def save(d, timetable):
    ad_data = pd.DataFrame( data=d )
    hour_data = ad_data.join( timetable, how='outer' )
    table = "all_time"
#     print(hour_data)
    mysql_adactivity_save.intoDB( table, hour_data )



# In[21]:


def value_predict(time_seq):
    print('[value predict]')
    
#     model = RnnModel.model
    time_seq = time_seq.values
    time_seq = time_seq.reshape( (1, 1, MODEL_SHAPE) )
    scaled_seq = scale( scalerX, time_seq )
    scaled_seq = scaled_seq.reshape( (1, 1, MODEL_SHAPE) )
    with graph.as_default():
        y = model.predict(scaled_seq)
    y = inverse_scale( scalerY, y )
    pred_cpc, pred_clicks = y[0][0], y[0][1]
    #print("y[0][0]:",y[0][0]," y[0][1]:",y[0][1])
    return pred_cpc, pred_clicks

def loading_model(MODEL_PATH):
    global graph
    model = RnnModel.model
    graph = tf.get_default_graph()
def predict_v(time_seq):
    time_seq = time_seq.values
    time_seq = time_seq.reshape((1,1,MODEL_SHAPE))
    scaled_seq = scale(scalerX, time_seq)
    scaled_seq = scaled_seq.reshape((1,1,MODEL_SHAPE))
    model = RnnModel.model
    md = model.summary()
    y = model.predict(scaled_seq)
    y = inverse_scale(scalerY, y)
    #print("y[0][0]:",y[0][0]," y[0][1]:",y[0][1])
    return y[0][0], y[0][1]

# In[22]:


def save_predict(ad_id, next_cpc, next_budget, pred_cpc, pred_clicks, speed, avgspeed, decide_type):
    request_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     print('[saving prediction]')
    d={'ad_id':[ ad_id ], 'request_time':[ request_time ],       
       'pred_cpc':[ pred_cpc ], 'pred_budget':[ next_budget ],
       'avgspeed':[ avgspeed ], 'speed':[ speed ],
       'pred_click':[ pred_clicks ], 'next_cpc':[ next_cpc ],
       'decide_type':[decide_type]
      }
    
    df2 = pd.DataFrame(data=d)
    table="pred"
    mysql_adactivity_save.intoDB( table, df2 )
#     print('pred data inserted')
    return

def save_revive(ad_id, next_cpc, daily_budget, decide_type, speed, avgspeed):
    request_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     print('[saving prediction]')
    d={'ad_id':[ ad_id ], 'request_time':[ request_time ],
       'next_cpc':[ next_cpc ], 'pred_budget':[ daily_budget ],
       'decide_type':[decide_type], 'speed':[ speed ], 'avgspeed': [ avgspeed ]
      }
    df2 = pd.DataFrame(data=d)
    table="pred"
    mysql_adactivity_save.intoDB( table, df2 )
    return
# In[23]:


def cpc_optimize(ad_id, time_seq):
    daily_budget = mysql_adactivity_save.get_init_budget(ad_id)
    last_cpc = time_seq[COL_CPC].iloc[0]
    pred_cpc, pred_clicks = value_predict(time_seq)
    next_cpc = np.mean( [ pred_cpc, last_cpc ] )
#     print( "pred_cpc:", pred_cpc )
#     print( "next_cpc:", next_cpc ) 
    return int( next_cpc ), int( daily_budget ), float( pred_cpc ), float( pred_clicks )


# In[24]:



def KPI_optimize(ad_id, time_seq, hour, sum_hour_click, daily_budget):
#     last_cpc = time_seq[COL_CPC].iloc[0]
    _, pred_click = value_predict(time_seq)
    pred_cpc = fb_graph.revive_bidding(ad_id)
    next_cpc = abs(pred_cpc)
#     print( "pred_cpc:", pred_cpc )
#     print( "next_cpc:", next_cpc )
    budget =( pred_click * (hour+1) - sum_hour_click ) * next_cpc
    return int( next_cpc ), int( daily_budget + budget ), float( pred_cpc ), float( pred_click )

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
    hour_ = np.zeros( (1, len(HOUR_AXIS) ), dtype=int )
    hour_[0][hour]=1
    hour_ = pd.DataFrame( data=hour_, columns=HOUR_AXIS )
    return hour_

# In[26]:


def scale(scalerX, x):
    x = x.reshape(1, MODEL_SHAPE)
    scaled_x = scalerX.transform(x)
    return scaled_x


# In[27]:


def inverse_scale(scalerY, y):
    y = np.reshape( y, (1, OUTPUT_SHAPE) )
    inverse_y = scalerY.inverse_transform(y)
    return inverse_y


# In[28]:


#計算走速(clicks/24hours)
def compute_speed(ad_id):
    df = mysql_adactivity_save.getSpeedData( ad_id )
    request_date = datetime.datetime.now().date()
    df[COL_REQUEST_TIME] = df[COL_REQUEST_TIME].dt.date
    df = df[df.request_time == request_date]
    if df.empty:
        speed = 0
    else:
        last_request_time = df[COL_REQUEST_TIME].iloc[0]
        clicks_sofar = df[COL_CLICKS].values[0]

        df[COL_DATE] = last_request_time
        df = df[[ COL_CLICKS, COL_DATE ]]

        speed = clicks_sofar / HOUR_PER_DAY
        speed = speed.astype(dtype=object)
    return speed

# In[19]:


def checkFile(col_names, data_path):
    print("[checkFile]")
    if not data_path.exists():
        df = pd.DataFrame(columns = col_names)
        df.to_csv(data_path , index=False)
def main():
    speed(23843180432950316)
    return
if __name__ == "__main__":
    main()



