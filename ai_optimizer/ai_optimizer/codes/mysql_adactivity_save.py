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

# import fb_graph
# In[ ]:
DATABASE="Facebook"

def connectDB(db_name):
    mydb = mysql.connector.connect(
        host="localhost",
        user="app",
        passwd="adgeek1234",
        database=db_name
    )
    return mydb


# In[ ]:

#by_campaign

def insertData(ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations, total_clicks):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO by_campaign ( ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations, request_time, total_clicks ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s )"
    val = ( ad_id, cpc, clicks, impressions, reach, campaign_id, spend_cap, objective, start_time, stop_time, adset_id, bid_amount, daily_budget, age_max, age_min, flexible_spec, geo_locations, datetime.datetime.now(), total_clicks )
    mycursor.execute(sql, val)
    mydb.commit()

def update_init_budget(ad_id, target_budget):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()

    sql = "UPDATE by_campaign SET daily_budget = %s WHERE ad_id = %s ORDER BY request_time LIMIT 1"
    val = ( target_budget, ad_id )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def getSpeedData(ad_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM by_campaign WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    return df

def getInsightData(ad_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT ad_id, cpc, clicks, impressions, reach, bid_amount FROM by_campaign WHERE ad_id=%s" % (ad_id), con=mydb )
    return 

def get_clicks_data(ad_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT total_clicks FROM by_campaign WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    return df.values[0,0]

def getDataforSelection(adset_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM by_campaign WHERE adset_id=%s" % (adset_id), con=mydb )
    return df

def get_campaign_id(ad_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT campaign_id FROM by_campaign WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    return df.iloc[0,0].astype(dtype=object)

def get_ad_data(ad_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT ad_id, cpc, clicks, impressions, reach, bid_amount, daily_budget FROM by_campaign WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    return df

def get_ad_id(campaign_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM by_campaign WHERE campaign_id = %s" % (campaign_id) , con=mydb )
    adid_list = df['ad_id'].unique()
    return adid_list
    
def get_init_budget(ad_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT daily_budget FROM by_campaign WHERE ad_id=%s ORDER BY request_time LIMIT 1" % (ad_id), con=mydb )
    daily_budget = df['daily_budget'].iloc[0].astype(dtype=object)
    
    return daily_budget

def get_init_bidamount(ad_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT bid_amount FROM by_campaign WHERE ad_id=%s ORDER BY request_time LIMIT 1" % (ad_id), con=mydb )
    bid_amount = df['bid_amount'].iloc[0].astype(dtype=object)
    return bid_amount

def get_init_cpc(ad_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT cpc FROM by_campaign WHERE ad_id=%s ORDER BY request_time LIMIT 1" % (ad_id), con=mydb )
    return df

def get_lastday_clicks(ad_id):
    mydb = connectDB(DATABASE)
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
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT total_clicks FROM by_campaign WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    total_clicks = mycursor.fetchall()
    
    total_clicks = int(total_clicks[0][0])
    return total_clicks

#pred

def update_init_bid(adset_id, init_bid):
    DB_LIST = ["ad_activity", "Facebook"]
    for database in DB_LIST:
        mydb = connectDB(database)
        mycursor = mydb.cursor()
        sql = "UPDATE adset_insights SET  bid_amount = %s WHERE adset_id = %s LIMIT 1"
        val = ( init_bid+1, adset_id )
        mycursor.execute(sql, val)
        mydb.commit()
    return

def update_avgspeed(ad_id, target_speed):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "UPDATE pred SET avgspeed = %s WHERE ad_id = %s ORDER BY request_time DESC LIMIT 1"
    val = ( target_speed, ad_id )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def get_speed(ad_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT avgspeed, speed, decide_type FROM pred WHERE ad_id=%s ORDER BY request_time DESC LIMIT 1" % (ad_id), con=mydb )
    avgspeed = df['avgspeed'].iloc[0]
    speed = df['speed'].iloc[0]
    decide_type = df['decide_type'].iloc[0]
    return avgspeed, speed, decide_type

def get_pred_data(ad_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM pred WHERE ad_id=%s" % (ad_id), con=mydb )
    if len(df) !=0:
        return df['next_cpc'].iloc[-1].astype(dtype=object), df['pred_budget'].iloc[-1].astype(dtype=object), df['decide_type'].iloc[-1]
    else:
        return None
    
#status

def insertSelection(campaign_id, adset_id, ad_id, selection):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO status ( campaign_id, adset_id, ad_id, status ) VALUES ( %s, %s, %s, %s )"
    val = ( campaign_id, adset_id, ad_id, selection )
    mycursor.execute(sql, val)
    mydb.commit()
    return

#default_price

def insert_default( campaign_id, mydict, datetime ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO default_price ( campaign_id, default_price, request_time ) VALUES ( %s, %s, %s )"
    val = ( campaign_id, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def get_default( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT default_price FROM default_price WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    default = mycursor.fetchall()
    default = str(default[0][0], encoding='utf-8')
    return default

#result

def insert_result( campaign_id, mydict, datetime ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO result ( campaign_id, result, request_time ) VALUES ( %s, %s, %s )"
    val = ( campaign_id, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def get_result( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT result FROM result WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    results = mycursor.fetchall()
    results = str(results[0][0], encoding='utf-8')
    return results

#all_time
    
def getAllTimeData(ad_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM all_time WHERE ad_id=%s" % (ad_id), con=mydb )
    return df
#default_price

def check_default_price(campaign_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM default_price WHERE campaign_id=%s" % (campaign_id), con=mydb )
    if df.empty:
        return True
    else:
        return False
#campaign_target

def check_campaignid_target(campaign_id, destination, charge_type):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id=%s" % (campaign_id), con=mydb )   
    if df.empty:
        mycursor = mydb.cursor()
        sql = "INSERT INTO campaign_target ( campaign_id, destination, charge_type ) VALUES ( %s, %s, %s )"
        val = ( campaign_id, destination, charge_type )
        mycursor.execute(sql, val)
        mydb.commit()
        return False
    else:
        sql = "UPDATE campaign_target SET destination=%s, charge_type=%s WHERE campaign_id=%s"
        val = ( destination, charge_type, campaign_id )
        mycursor = mydb.cursor()
        mycursor.execute(sql, val)
        mydb.commit()
        return True

def get_campaign_target_dict():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
    campaignid_dict=dict()
    campaignid_list = df['campaign_id'].unique()
    for campaign_id in campaignid_list:
        stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
        if stop_time >= request_time:
            campaignid_dict[campaign_id]=df['destination'][df.campaign_id==campaign_id]
    return campaignid_dict

def get_campaign_target_left_dict():
    DATABASE = "ad_activity"
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
    campaignid_dict=dict()
    campaignid_list = df['campaign_id'].unique()
    for campaign_id in campaignid_list:
#         print(df[df.campaign_id==campaign_id])
        stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
        if stop_time >= request_time:
            campaignid_dict[campaign_id]=df['target_left'][df.campaign_id==campaign_id]
            
    return campaignid_dict

def get_campaign():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
    campaign_id_list = df['campaign_id'].unique()
    return campaign_id_list
### optimal_weight ###

def check_optimal_weight(campaign_id, df):
    mydb = connectDB(DATABASE)
    df_check = pd.read_sql( "SELECT * FROM optimal_weight WHERE campaign_id=%s" % (campaign_id), con=mydb )
#     print(type(campaign_id.astype(dtype=object)))
    if df_check.empty:
        engine = create_engine( 'mysql://app:adgeek1234@localhost/Facebook' )
        with engine.connect() as conn, conn.begin():
            df.to_sql( "optimal_weight", conn, if_exists='append',index=False )
        return
    else:
        mycursor = mydb.cursor()
        sql = "UPDATE optimal_weight SET score=%s, weight_kpi=%s, weight_spend=%s, weight_bid=%s WHERE campaign_id=%s"
        val = ( df['score'].iloc[0].astype(dtype=object),
                df['weight_kpi'].iloc[0].astype(dtype=object),
                df['weight_spend'].iloc[0].astype(dtype=object),
                df['weight_bid'].iloc[0].astype(dtype=object),
                df['campaign_id'].iloc[0].astype(dtype=object)
              )
#         sql = "UPDATE optimal_weight SET score=%s, weight_kpi=%s, weight_spend=%s, weight_bid=%s, weight_width=%s WHERE campaign_id=%s"
#         val = ( df['score'].iloc[0].astype(dtype=object),
#                 df['weight_kpi'].iloc[0].astype(dtype=object),
#                 df['weight_spend'].iloc[0].astype(dtype=object),
#                 df['weight_bid'].iloc[0].astype(dtype=object),
#                 df['weight_width'].iloc[0].astype(dtype=object),
#                 df['campaign_id'].iloc[0].astype(dtype=object)
#               )
        mycursor.execute(sql, val)
        mydb.commit()
        return
######## NEW ######

def get_campaign_target(campaign_id):
    mydb = connectDB(DATABASE)
    df_camp = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id=%s" % (campaign_id), con=mydb )
    return df_camp

def update_campaign_target(df_camp):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    try:
        sql = ("UPDATE campaign_target SET charge_type = %s, cost_per_target = %s, daily_budget = %s, daily_charge = %s, destination = %s, impressions = %s, period = %s, reach = %s, spend = %s, spend_cap = %s, start_time = %s , stop_time=%s, target=%s, target_left=%s, target_type=%s WHERE campaign_id = %s")
        val = ( 
            df_camp['charge_type'].iloc[0],
            df_camp['cost_per_target'].iloc[0].astype(dtype=object),
            df_camp['daily_budget'].iloc[0].astype(dtype=object),
            df_camp['daily_charge'].iloc[0].astype(dtype=object),
            df_camp['destination'].iloc[0].astype(dtype=object),
            df_camp['impressions'].iloc[0].astype(dtype=object),
            df_camp['period'].iloc[0].astype(dtype=object),
            df_camp['reach'].iloc[0].astype(dtype=object),
            df_camp['spend'].iloc[0].astype(dtype=object),
            df_camp['spend_cap'].iloc[0].astype(dtype=object),
            df_camp['start_time'].iloc[0],
            df_camp['stop_time'].iloc[0],
            df_camp['target'].iloc[0].astype(dtype=object),
            df_camp['target_left'].iloc[0].astype(dtype=object),
            df_camp['target_type'].iloc[0],
            df_camp['campaign_id'].iloc[0].astype(dtype=object)
        )
    except:
        sql = "UPDATE campaign_target SET target_type = %s,spend_cap = %s, start_time = %s, stop_time = %s , period=%s, daily_budget=%s WHERE campaign_id = %s"
        val = ( 
            df_camp['target_type'].iloc[0], 
            df_camp['spend_cap'].iloc[0], 
            df_camp['start_time'].iloc[0], 
            df_camp['stop_time'].iloc[0], 
            df_camp['period'].iloc[0].astype(dtype=object), 
            df_camp['daily_budget'].iloc[0].astype(dtype=object), 
            df_camp['campaign_id'].iloc[0]
        )
    mycursor.execute(sql, val)
    mydb.commit()
    return

def intoDB(table, df):
    engine = create_engine( 'mysql://app:adgeek1234@localhost/Facebook' )
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
        elif table == "optimal_weight":
            df.to_sql("optimal_weight", conn, if_exists='append',index=False)
        elif table == "adset_score":
            df.to_sql("adset_score", conn, if_exists='append',index=False)

if __name__=="__main__":
    get_campaign_target()