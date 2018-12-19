
# coding: utf-8

import mysql_adactivity_save
import pandas as pd
import numpy as np
import datetime
import fb_graph
from sklearn.externals import joblib

FOLDER_PATH = '/storage/opt_project_test/optProjectTest/optProjectTest/models/performance_models/'
MODEL_PATH = FOLDER_PATH + 'bayesian_gaussian_mixture_exclude0.pkl'
PERFORMANCE = ['cpc', 'clicks', 'impressions', 'reach']


def performance_sort( adset_id ):
    request_date = datetime.datetime.now().date()
    mydb = mysql_adactivity_save.connectDB("ad_activity")
    df_adset = pd.read_sql( "SELECT * FROM ad_insights WHERE adset_id=%s" % (adset_id), con=mydb )
    df_adset['request_time']=pd.to_datetime(df_adset['request_time'])
    df_adset = df_adset[df_adset.request_time.dt.date==request_date]
    if df_adset.empty == False:
        df_adset = pd.DataFrame({'adset_id':adset_id, 'charge':df_adset['charge'].iloc[-1]}, index=[0])
    else:
        df_adset = pd.DataFrame({'adset_id':adset_id, 'charge':0}, index=[0])
    return df_adset


def forOPT(adset_id, ad_id):
    df = mysql_adactivity_save.getDataforSelection(adset_id)
    d={}
    ad_id_list = df['ad_id'].unique().astype(int)
    if int(ad_id) in ad_id_list:
#         print('ad_id found')
        for ad in ad_id_list:
            d[ad] = df['clicks'][df['ad_id'] == ad].sort_values().iloc[-1]
        biggestKey = max(d, key=d.get)

        adid = int(df['ad_id'][df['ad_id'] == int(ad_id)].iloc[-1])
        if adid == int(biggestKey):
            return True
        else:
            return True#!!!!!!!!!!!記得改回False
    else:
#         print('ad_id not found')
        return True#!!!!!!!!!!!記得改回False
    
def performance_compute(adset_id, ad_id):
    mixure_model = joblib.load( MODEL_PATH )
    df = mysql_adactivity_save.getDataforSelection( adset_id )
    YESTERDAY = datetime.datetime.today().date() - datetime.timedelta(1)
    if not df.empty:
        df = df[ df.request_time.dt.date == YESTERDAY ]
        if not df.empty:
            ad_id_list = df['ad_id'].unique().astype(int)
            d=dict()
            for ad in ad_id_list:
                hour_list = df['request_time'][ df['ad_id'] == ad ].dt.hour.unique()
                performance_series = df[ PERFORMANCE ][ df['ad_id'] == ad ].iloc[0,:]
                performance = performance_series.values.reshape(1, -1) / len( hour_list )
                ranking = mixure_model.predict_proba( performance )[0][1]
                d[ ad ] = ranking
            biggestKey = max(d, key=d.get)
            if int(ad_id) == int(biggestKey):
                return True
            else:
                return True#!!!!!!!!!!!記得改回False
        else:
            return True#!!!!!!!!!!!記得改回False
    else:
        print('ad_id not found')
        return True#!!!!!!!!!!!記得改回False   
        



def checkAdSpeed(ad_id, TOTAL_CLICKS):
    return

if __name__=='__main__':
    
    
    forOPT(23842974131840246, 23842974131850246)