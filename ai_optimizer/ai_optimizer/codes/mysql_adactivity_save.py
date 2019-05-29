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
import sys

BRANDING_CAMPAIGN_LIST = [
    'LINK_CLICKS', 'ALL_CLICKS','VIDEO_VIEWS', 'REACH', 'POST_ENGAGEMENT', 'PAGE_LIKES', 'LANDING_PAGE_VIEW']
PERFORMANCE_CAMPAIGN_LIST = [
    'CONVERSIONS', 'LEAD_GENERATION', 'ADD_TO_CART']
# import fb_graph
# In[ ]:
DATABASE="dev_facebook_test"
HOST = "aws-prod-ai-private.adgeek.cc"
def connectDB(db_name):
    mydb = mysql.connector.connect(
        host=HOST,
        user="app",
        passwd="adgeek1234",
        database=db_name
    )
    return mydb


# In[ ]:

    
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
    mycursor.close()
    mydb.close()
    return

def get_default( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT default_price FROM default_price WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    default = mycursor.fetchall()
    default = str(default[0][0], encoding='utf-8')
    mycursor.close()
    mydb.close()
    return default

def check_default_price(campaign_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM default_price WHERE campaign_id=%s" % (campaign_id), con=mydb )
    mydb.close()
    if df.empty:
        return True
    else:
        return False
#result

def insert_result( campaign_id, mydict, datetime ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO result ( campaign_id, result, request_time ) VALUES ( %s, %s, %s )"
    val = ( campaign_id, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()
    return

def get_result( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT result FROM result WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    results = mycursor.fetchall()
    results = str(results[0][0], encoding='utf-8')
    mycursor.close()
    mydb.close()
    return results

#campaign_target

def check_campaignid_target(campaign_id, destination, charge_type, ai_start_date, ai_stop_date, ai_spend_cap):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id=%s" % (campaign_id), con=mydb )   
    if df.empty:
        mycursor = mydb.cursor()
        sql = "INSERT INTO campaign_target ( campaign_id, destination, charge_type, ai_start_date, ai_stop_date, ai_spend_cap ) VALUES ( %s, %s, %s, %s, %s, %s )"
        val = ( campaign_id, destination, charge_type, ai_start_date, ai_stop_date, ai_spend_cap )
        mycursor.execute(sql, val)
        mydb.commit()
        mycursor.close()
        mydb.close()
        return False
    else:
        sql = "UPDATE campaign_target SET destination=%s, charge_type=%s, ai_start_date=%s, ai_stop_date=%s, ai_spend_cap=%s WHERE campaign_id=%s"
        val = ( destination, charge_type, ai_start_date, ai_stop_date, ai_spend_cap, campaign_id )
        mycursor = mydb.cursor()
        mycursor.execute(sql, val)
        mydb.commit()
        mycursor.close()
        mydb.close()
        return True

def get_campaign_target_dict():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
    campaignid_dict=dict()
    campaignid_list = df['campaign_id'].unique()
    for campaign_id in campaignid_list:
        campaignid_dict[campaign_id]=df['destination'][df.campaign_id==campaign_id]
#         stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
#         if stop_time >= request_time:
#             campaignid_dict[campaign_id]=df['destination'][df.campaign_id==campaign_id]
    mydb.close()
    return campaignid_dict

def get_campaign_target_left_dict():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
    mydb.close()
    campaignid_dict=dict()
    campaignid_list = df['campaign_id'].unique()
    for campaign_id in campaignid_list:
#         print(df[df.campaign_id==campaign_id])
        stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
        if stop_time >= request_time:
            campaignid_dict[campaign_id]=df['target_left'][df.campaign_id==campaign_id]
    return campaignid_dict

### optimal_weight ###

def check_optimal_weight(campaign_id, df):
    mydb = connectDB(DATABASE)
    df_check = pd.read_sql( "SELECT * FROM optimal_weight WHERE campaign_id={}".format(campaign_id), con=mydb )
    if df_check.empty:
        engine = create_engine( 'mysql://app:adgeek1234@{}/{}'.format( HOST, DATABASE ) )
        with engine.connect() as conn, conn.begin():
            df.to_sql( "optimal_weight", conn, if_exists='append',index=False )
            engine.dispose()
        return
    else:
        mycursor = mydb.cursor()
        campaign_id = df['campaign_id'].iloc[0]
        df.drop(['campaign_id'], axis=1)
        for column in df.columns:
            try:
                sql = ("UPDATE optimal_weight SET {}='{}' WHERE campaign_id={}".format( column, df[column].iloc[0], campaign_id))
                mycursor.execute(sql)
                mydb.commit()
            except Exception as e:
                print(e)
                pass
        mycursor.close()
        mydb.close()
        return

######## NEW ######
def get_campaign_target(campaign_id=None):
    mydb = connectDB(DATABASE)
    request_date = datetime.datetime.now().date()
    if campaign_id:
        df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id='{}'".format(campaign_id), con=mydb )
    else:
        df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
        
    df_is_running = df.drop( df[ df['ai_stop_date'] < request_date].index )
    return df_is_running

def get_campaigns_not_optimized():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now().date()

    df_not_optimized = pd.read_sql( 
        "SELECT * FROM campaign_target where ai_stop_date>='{0}' and  (optimized_date <> '{0}' or optimized_date is null )  ".format(request_time),
        con=mydb )
        
    return df_not_optimized


def get_campaign(campaign_id=None):
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    if campaign_id is None:
        df = pd.read_sql( "SELECT * FROM campaign_target", con=mydb )
        mydb.close()
        return df
    else:
        df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id='{}'".format(campaign_id), con=mydb )
        df_camp = pd.DataFrame(columns=df.columns)
        stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
        if stop_time.date() >= request_time.date():
            df_camp= pd.concat( [ df_camp, df[df.campaign_id==campaign_id] ], axis=0 )
        mydb.close()
        return df_camp
    
def get_running_branding_campaign(campaign_id=None):
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    if campaign_id is None:
        df = pd.read_sql( "SELECT * FROM campaign_target", con=mydb )
        df = df[ (df['target_type'].isin(BRANDING_CAMPAIGN_LIST)) & (df.stop_time >= request_time) ]
        mydb.close()
        return df
    else:
        df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id='{}'".format(campaign_id), con=mydb )
        mydb.close()
        return df

    
def update_campaign_target(df):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    campaign_id = df['campaign_id'].iloc[0]
    df.drop(['campaign_id'], axis=1)
    for column in df.columns:
        try:
            sql = ("UPDATE campaign_target SET {}='{}' WHERE campaign_id={}".format( column, df[column].iloc[0], campaign_id) )
            mycursor.execute(sql)
            mydb.commit()
        except Exception as e:
            print(e)
            pass
    mycursor.close()
    mydb.close()
    return

def intoDB(table, df):
    engine = create_engine( 'mysql://app:adgeek1234@{}/{}'.format(HOST, DATABASE) )
#     print(df.columns)
    with engine.connect() as conn, conn.begin():
        df.to_sql(table, conn, if_exists='append',index=False)
        engine.dispose()
            
def insert_release_result( campaign_id, mydict, datetime ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO release_ver_result ( campaign_id, result, request_time ) VALUES ( %s, %s, %s )"
    val = ( campaign_id, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()
    return

def get_release_result( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT result FROM release_ver_result WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    results = mycursor.fetchall()
    try:
        results = str(results[0][0], encoding='utf-8')
        mycursor.close()
        mydb.close()
        return results
    except:
        mycursor.close()
        mydb.close()
        return str(dict())

def insert_release_default( campaign_id, mydict, datetime ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO release_default_price ( campaign_id, default_price, request_time ) VALUES ( %s, %s, %s )"
    val = ( campaign_id, mydict, datetime )
    mycursor.execute(sql, val)
    mydb.commit()
    mycursor.close()
    mydb.close()
    return

def get_release_default( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    mycursor.execute( "SELECT default_price FROM release_default_price WHERE campaign_id=%s ORDER BY request_time DESC LIMIT 1" % (campaign_id) )
    default = mycursor.fetchall()
    try:
        default = str(default[0][0], encoding='utf-8')
        mycursor.close()
        mydb.close()
        return default
    except:
        return str(dict())

def check_release_default_price(campaign_id):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM release_default_price WHERE campaign_id=%s" % (campaign_id), con=mydb )
    mydb.close()
    if df.empty:
        return True
    else:
        return False

def update_init_bid(adset_id, init_bid):
#     print(adset_id, ': ', init_bid)
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "UPDATE adset_initial_bid SET bid_amount={} WHERE adset_id={} LIMIT 1".format(init_bid, adset_id)
    mycursor.execute(sql)
    mydb.commit()
    mycursor.close()
    mydb.close()
    return

def adjust_init_bid(campaign_id, up_ratio=1.1):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    ### select
    sql = "SELECT bid_amount FROM adset_initial_bid WHERE campaign_id={}".format(campaign_id)
    mycursor.execute(sql)
    init_bid = mycursor.fetchall()
    init_bid = int(init_bid[0][0])
    if init_bid > 100:
        init_bid = init_bid * up_ratio
    else:
        init_bid += 1
    ### update
    sql = "UPDATE adset_initial_bid SET bid_amount={} WHERE campaign_id={}".format(init_bid, campaign_id)
    mycursor.execute(sql)
    mydb.commit()
    mycursor.close()
    mydb.close()    
    return



def check_initial_bid(adset_id, df):
    mydb = connectDB(DATABASE)
    df_check = pd.read_sql( "SELECT * FROM adset_initial_bid WHERE adset_id={}".format(adset_id), con=mydb )
#     print(type(campaign_id.astype(dtype=object)))
    if df_check.empty:
        engine = create_engine( 'mysql://app:adgeek1234@{}/{}'.format( HOST, DATABASE ) )
        with engine.connect() as conn, conn.begin():
            df.to_sql( "adset_initial_bid", conn, if_exists='append',index=False )
        engine.dispose()
    return
    
def update_init_bid_by_campaign(campaign_id, init_bid):
    df_camp = get_campaign_target(campaign_id)
    adset_list = Campaigns(campaign_id, df_camp['charge_type'].iloc[0]).get_adsets()
    for adset_id in adset_list:
        update_init_bid( int(adset_id), init_bid )
    return

def get_campaign_ai_brief( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql =  "SELECT ai_spend_cap, ai_start_date, ai_stop_date FROM campaign_target WHERE campaign_id={}".format(campaign_id)

    mycursor.execute(sql)
    field_name = [field[0] for field in mycursor.description]
    values = mycursor.fetchone()
    row = dict(zip(field_name, values))
    spend_cap = row['ai_spend_cap']
    start_date = row['ai_start_date']
    stop_date = row['ai_stop_date']
    mycursor.close()
    mydb.close()
    return row

if __name__=="__main__":

    print(sys.argv)
    from facebook_datacollector import Campaigns
    if len(sys.argv) < 2:
        print('Error, the format should be: ', 'python3 mysql_adactivity_save.py get_ai_brief [campaign_id] ')
        print('Error, the format should be: ', 'python3 mysql_adactivity_save.py update_init_bid [campaign_id] [bid_value] ')
    
    current_operation = sys.argv[1]
    print('current_operation:' ,current_operation)
    if current_operation == 'get_ai_brief':
        campaign_id = sys.argv[2]
        result = get_campaign_ai_brief(campaign_id)
        print(result)
    elif current_operation == 'update_init_bid':
        campaign_id = sys.argv[2]
        init_bid = sys.argv[3]
        update_init_bid_by_campaign(campaign_id, init_bid)
    else:
        print('error')


    
    