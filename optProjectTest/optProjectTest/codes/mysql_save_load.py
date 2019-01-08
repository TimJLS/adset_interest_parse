#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import mysql.connector
import pandas as pd
import datetime
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb


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
    mydb = connectDB("tim_test")
    mycursor = mydb.cursor()
    sql = "INSERT INTO by_campaign ( ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations, request_time, total_clicks ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s )"
    val = ( ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations, datetime.datetime.now(), total_clicks )
    mycursor.execute(sql, val)
    mydb.commit()
    print('[ad_id : ', ad_id, 'inserted]')
    print(mycursor.rowcount, "record inserted")
#insertData(19910630, 5.0, 10, 100, 99, 12)

def insertSelection(campaign_id, adset_id, ad_id, selection):
    mydb = connectDB("tim_test")
    mycursor = mydb.cursor()
    sql = "INSERT INTO status ( campaign_id, adset_id, ad_id, status ) VALUES ( %s, %s, %s, %s )"
    val = ( campaign_id, adset_id, ad_id, selection )
    mycursor.execute(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "selection results inserted")
    
def getSpeedData(ad_id):
    mydb = connectDB("tim_test")
    df = pd.read_sql( "SELECT * FROM by_campaign WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    return df
#getData(19910630)
def getInsightData(ad_id):
    mydb = connectDB("tim_test")
    df = pd.read_sql( "SELECT ad_id, cpc, clicks, impressions, reach, bid_amount FROM by_campaign WHERE ad_id=%s" % (ad_id), con=mydb )
    return 
    
def getAllTimeData(ad_id):
    mydb = connectDB("tim_test")
    df = pd.read_sql( "SELECT * FROM all_time WHERE ad_id=%s" % (ad_id), con=mydb )
    return df

def getClicksData(ad_id):
    mydb = connectDB("tim_test")
    df = pd.read_sql( "SELECT total_clicks FROM by_campaign WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    return df.values[0,0]

def getDataforSelection(adset_id):
    mydb = connectDB("tim_test")
    df = pd.read_sql( "SELECT * FROM by_campaign WHERE adset_id=%s" % (adset_id), con=mydb )
    return df

def getPredData(ad_id):
    mydb = connectDB("tim_test")
    df = pd.read_sql( "SELECT * FROM pred WHERE ad_id=%s" % (ad_id), con=mydb )
    if len(df) !=0:
        return df['pred_cpc'].iloc[-1], df['pred_budget'].iloc[-1]
    else:
        return None


def intoDB(table, df):
    engine = create_engine( 'mysql://root:adgeek1234@localhost/tim_test' )
    with engine.connect() as conn, conn.begin():
        if table == "pred":
            df.to_sql("pred", conn, if_exists='append',index=False)
        else:
            df.to_sql("all_time", conn, if_exists='append',index=False)


if __name__=="__main__":
    #intoDB()
#     AdsInAdSet(adset_id)
#     insertData(ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget)
    print(getData(str(23842974131850246)))
    
