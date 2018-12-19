#!/usr/bin/env python
# coding: utf-8
#===================================================================#
#===================================================================#
# In[ ]:


import mysql.connector
import pandas as pd
import datetime
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

import fb_graph
# In[ ]:


def connectDB(db_name):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="adgeek1234",
        database=db_name
    )
    return mydb
#connectDB("ad_activity")


# In[ ]:


def selectData(table):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT * FROM %s" %(table) )
    results = mycursor.fetchall()
    
#    for record in results:
#        print(record)
#        print("%s, %s, %s" %(col1, col2, col3))


# In[ ]:


def insertData(ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations, total_clicks):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    sql = "INSERT INTO by_campaign ( ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations, request_time, total_clicks ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s )"
    val = ( ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations, datetime.datetime.now(), total_clicks )
    mycursor.execute(sql, val)
    mydb.commit()
#     print('[ad_id : ', ad_id, 'inserted]')
#     print(mycursor.rowcount, "record inserted")
#insertData(19910630, 5.0, 10, 100, 99, 12)

def insertSelection(campaign_id, adset_id, ad_id, selection):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    sql = "INSERT INTO status ( campaign_id, adset_id, ad_id, status ) VALUES ( %s, %s, %s, %s )"
    val = ( campaign_id, adset_id, ad_id, selection )
    mycursor.execute(sql, val)
    mydb.commit()
#     print(mycursor.rowcount, "selection results inserted")

def insert_result( campaign_id, mydict, datetime ):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    sql = "INSERT INTO result ( campaign_id, result, request_time ) VALUES ( %s, %s, %s )"
    val = ( campaign_id, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def get_result( campaign_id ):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT result FROM result WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    results = mycursor.fetchall()
    results = str(results[0][0], encoding='utf-8')
    return results

def insert_default( campaign_id, mydict, datetime ):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    sql = "INSERT INTO default_price ( campaign_id, default_price, request_time ) VALUES ( %s, %s, %s )"
    val = ( campaign_id, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def update_bidcap(ad_id, bid):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    sql = "UPDATE pred SET next_cpc = %s WHERE ad_id = %s ORDER BY request_time DESC LIMIT 1"
    val = ( bid, ad_id )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def update_init_budget(ad_id, target_budget):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    sql = "UPDATE by_campaign SET daily_budget = %s WHERE ad_id = %s ORDER BY request_time LIMIT 1"
    val = ( target_budget, ad_id )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def update_avgspeed(ad_id, target_speed):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    sql = "UPDATE pred SET avgspeed = %s WHERE ad_id = %s ORDER BY request_time DESC LIMIT 1"
    val = ( target_speed, ad_id )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def get_default( campaign_id ):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT default_price FROM default_price WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    default = mycursor.fetchall()
    default = str(default[0][0], encoding='utf-8')
    return default

def get_speed(ad_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT avgspeed, speed, decide_type FROM pred WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    if df.empty == False:
        avgspeed = df['avgspeed'].iloc[0]
        speed = df['speed'].iloc[0]
        decide_type = df['decide_type'].iloc[0]
        return avgspeed, speed, decide_type
    else:
        avgspeed = 0
        speed = 0
        decide_type = 'KPI'
        return avgspeed, speed, decide_type

def getSpeedData(ad_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT * FROM by_campaign WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    return df

def getInsightData(ad_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT ad_id, cpc, clicks, impressions, reach, bid_amount FROM by_campaign WHERE ad_id=%s" % (ad_id), con=mydb )
    return 
    
def getAllTimeData(ad_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT * FROM all_time WHERE ad_id=%s" % (ad_id), con=mydb )
    return df

def getClicksData(ad_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT total_clicks FROM by_campaign WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    return df.values[0,0]

def getDataforSelection(adset_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT * FROM by_campaign WHERE adset_id=%s" % (adset_id), con=mydb )
    return df

def get_campaign_id(ad_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT campaign_id FROM by_campaign WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    return df.iloc[0,0].astype(dtype=object)

def get_ad_data(ad_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT ad_id, cpc, clicks, impressions, reach, bid_amount, daily_budget FROM by_campaign WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    return df

def get_pred_data(ad_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT * FROM pred WHERE ad_id=%s" % (ad_id), con=mydb )
    if len(df) !=0:
        return df['next_cpc'].iloc[-1].astype(dtype=object), df['pred_budget'].iloc[-1].astype(dtype=object), df['decide_type'].iloc[-1]
    else:
        return None

def check_campaignid_target(campaign_id, total_clicks):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id=%s" % (campaign_id), con=mydb )
    avgspeed = fb_graph.getAvgSpeed( campaign_id, total_clicks )
    if df.empty:
        mycursor = mydb.cursor()
        sql = "INSERT INTO campaign_target ( campaign_id, target, avgspeed ) VALUES ( %s, %s, %s )"
        val = ( campaign_id, total_clicks, avgspeed )
        mycursor.execute(sql, val)
        mydb.commit()
    else:
        mycursor = mydb.cursor()
        sql = "UPDATE campaign_target SET target=%s, avgspeed=%s WHERE campaign_id=%s"
        val = ( total_clicks, avgspeed, campaign_id )
        mycursor.execute(sql, val)
        mydb.commit()
    return

<<<<<<< HEAD
def get_campaign_target_dict():
    mydb = connectDB("ad_activity")
    request_time = datetime.datetime.now()
=======
def get_campaignid_target():
    mydb = connectDB("ad_activity")
>>>>>>> 99d9fe34978fb78d0155703ef2693419869d888c
    df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
    campaignid_dict=dict()
    campaignid_list = df['campaign_id'].unique()
    for campaign_id in campaignid_list:
<<<<<<< HEAD
        stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
        if stop_time > request_time:
            campaignid_dict[campaign_id]=df['target'][df.campaign_id==campaign_id]
=======
        campaignid_dict[campaign_id]=df['target'][df.campaign_id==campaign_id]
>>>>>>> 99d9fe34978fb78d0155703ef2693419869d888c
    return campaignid_dict

def get_ad_id(campaign_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT * FROM by_campaign WHERE campaign_id = %s" % (campaign_id) , con=mydb )
    adid_list = df['ad_id'].unique()
    return adid_list
    
def get_init_budget(ad_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT daily_budget FROM by_campaign WHERE ad_id=%s ORDER BY request_time LIMIT 1" % (ad_id), con=mydb )
    daily_budget = df['daily_budget'].iloc[0].astype(dtype=object)
    
    return daily_budget

def get_init_bidamount(ad_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT bid_amount FROM by_campaign WHERE ad_id=%s ORDER BY request_time LIMIT 1" % (ad_id), con=mydb )
    bid_amount = df['bid_amount'].iloc[0].astype(dtype=object)
    return bid_amount

def get_lastday_clicks(ad_id):
    mydb = connectDB("ad_activity")
    df = pd.read_sql( "SELECT clicks, request_time FROM by_campaign WHERE ad_id=%s ORDER BY request_time" % (ad_id), con=mydb )
    last_day = datetime.datetime.now().date() - datetime.timedelta(1)
    df['request_time'] = df['request_time'].dt.date
    if df['clicks'][df.request_time ==last_day].empty:
        last_day = last_day - datetime.timedelta(1)
        lastday_clicks = 0
    else:
        lastday_clicks = df['clicks'][df.request_time ==last_day].iloc[-1].astype(dtype=object)
    return lastday_clicks

def get_total_clicks( campaign_id ):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT total_clicks FROM by_campaign WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    total_clicks = mycursor.fetchall()
    
    total_clicks = int(total_clicks[0][0])
    return total_clicks
<<<<<<< HEAD
######## NEW ######

def get_campaign_target(campaign_id):
    mydb = connectDB("ad_activity")
    df_camp = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id=%s" % (campaign_id), con=mydb )
    return df_camp

def update_campaign_target(df_camp):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    sql = "UPDATE campaign_target SET target_type = %s,spend_cap = %s, start_time = %s, stop_time = %s WHERE campaign_id = %s"
    val = ( df_camp['target_type'].iloc[0], df_camp['spend_cap'].iloc[0], df_camp['start_time'].iloc[0], df_camp['stop_time'].iloc[0], df_camp['campaign_id'].iloc[0] )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def intoDB(table, df):
    engine = create_engine( 'mysql://root:adgeek1234@localhost/ad_activity' )
#     print(df.columns)
    with engine.connect() as conn, conn.begin():
        if table == "pred":
            df.to_sql("pred", conn, if_exists='append',index=False)
        elif table == "ad_insights":
            df.to_sql("ad_insights", conn, if_exists='append',index=False)
        elif table == "adset_insights":
            df.to_sql("adset_insights", conn, if_exists='append',index=False)
        elif table == "campaign_target":
            df.to_sql("campaign_target", conn, if_exists='append',index=False)
        elif table == "by_campaign":
            df.to_sql("by_campaign", conn, if_exists='append',index=False)
        elif table == "all_time":
            df.to_sql("all_time", conn, if_exists='append',index=False)



=======

def intoDB(table, df):
    engine = create_engine( 'mysql://root:adgeek1234@localhost/ad_activity' )
    with engine.connect() as conn, conn.begin():
        if table == "pred":
            df.to_sql("pred", conn, if_exists='append',index=False)
        else:
            df.to_sql("all_time", conn, if_exists='append',index=False)


>>>>>>> 99d9fe34978fb78d0155703ef2693419869d888c
if __name__=="__main__":
    #intoDB()
#     AdsInAdSet(adset_id)
#     insertData(ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget)
    get_lastday_clicks(23843180432910316)
    
