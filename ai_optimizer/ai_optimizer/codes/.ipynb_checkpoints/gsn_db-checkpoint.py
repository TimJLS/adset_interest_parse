#!/usr/bin/env python
# coding: utf-8

# In[7]:


import mysql.connector
import pandas as pd
import datetime
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

DATABASE="dev_gsn"
HOST = "aws-prod-ai-private.adgeek.cc"
USER = "app"
PASSWORD = "adgeek1234"
def connectDB(db_name):
    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        passwd=PASSWORD,
        database=db_name
    )
    return mydb

def into_table(df, table=None):
    engine = create_engine( 'mysql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE) )
    with engine.connect() as conn, conn.begin():
        df.to_sql(table, conn, if_exists='append',index=False)
        engine.dispose()

def get_table(campaign_id=None, table=None):
    engine = create_engine( 'mysql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE) )
    with engine.connect() as conn, conn.begin():
        if campaign_id:
            df = pd.read_sql("SELECT * FROM {} WHERE campaign_id='{}'".format(table, campaign_id), con=conn)
        else:
            df = pd.read_sql("SELECT * FROM {}".format(table), con=conn)
    engine.dispose()
    return df

def update_table(df, table=None):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    campaign_id = df['campaign_id'].iloc[0]
    df.drop(['campaign_id'], axis=1)
    for column in df.columns:
        try:
            sql = ("UPDATE {} SET {}='{}' WHERE campaign_id={}".format(table, column, df[column].iloc[0], campaign_id))
            mycursor.execute(sql)
            mydb.commit()
        except Exception as e:
            print('[gsn_db.update_table]: ', e)
            pass
    mycursor.close()
    mydb.close()
    return

def get_campaign(campaign_id=None):
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    if campaign_id is None:
        df = pd.read_sql( "SELECT * FROM campaign_target WHERE ai_status='active'", con=mydb )
        df_is_running = df[df.ai_stop_date >= request_time.date()]
    else:
        df_is_running = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id='{}' AND 'ai_status'='active'".format(campaign_id), con=mydb )
    mydb.close()
    return df_is_running


def get_campaign_is_running(campaign_id=None):
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    if campaign_id is None:
        df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
        df_is_running = df.drop( df[ df['ai_stop_date'] < request_time.date() ].index )
    else:
        df_is_running = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id='{}'".format(campaign_id), con=mydb )
    mydb.close()
    return df_is_running

def get_branding_campaign_is_running(campaign_id=None):
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    if campaign_id is None:
        df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
        df_branding_campaign_is_running = df[ (df['ai_stop_date']>=request_time.date())&(df['destination_type']=='LINK_CLICKS') ]
    else:
        df_branding_campaign_is_running = pd.read_sql( 
            "SELECT * FROM campaign_target WHERE campaign_id='{}'".format(campaign_id), con=mydb )
    mydb.close()
    return df_branding_campaign_is_running

def get_performance_campaign_is_running(campaign_id=None):
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    if campaign_id is None:
        df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
        df_performance_campaign_is_running = df[ (df['ai_stop_date']>=request_time.date())&(df['destination_type']=='CONVERSIONS') ]
    else:
        df_performance_campaign_is_running = pd.read_sql( 
            "SELECT * FROM campaign_target WHERE campaign_id='{}'".format(campaign_id), con=mydb )
    mydb.close()
    return df_performance_campaign_is_running

def get_campaign_target(campaign_id=None):
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    if campaign_id:
        df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id='{}'".format(campaign_id), con=mydb )
    else:
        df = pd.read_sql( "SELECT * FROM campaign_target" , con=mydb )
        
    df_is_running = df[ df['ai_stop_date'] >= request_time.date()]
    mydb.close()
    return df_is_running

def get_campaigns_not_optimized():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now().date()

    df_not_optimized = pd.read_sql( 
        "SELECT * FROM campaign_target where ai_stop_date>='{0}' and  (optimized_date <> '{0}' or optimized_date is null )  ".format(request_time),
        con=mydb )
    mydb.close()
    return df_not_optimized



def check_campaignid_target(
    account_id, campaign_id, destination, destination_type, ai_status, ai_start_date, ai_stop_date, ai_spend_cap, destination_max):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id='{}'".format(campaign_id), con=mydb )
    mycursor = mydb.cursor()
    if df.empty:
        sql = "INSERT INTO campaign_target ( customer_id, campaign_id, destination, destination_type, ai_status, ai_start_date, ai_stop_date, ai_spend_cap, destination_max ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s )"
        val = ( account_id, campaign_id, destination, destination_type, ai_status, ai_start_date, ai_stop_date, ai_spend_cap, destination_max )
        mycursor.execute(sql, val)
        mydb.commit()
        mycursor.close()
        mydb.close()
        return False
    else:
        sql = "UPDATE campaign_target SET destination_max=%s, destination=%s, destination_type=%s, ai_status=%s, ai_start_date=%s, ai_stop_date=%s, ai_spend_cap=%s WHERE campaign_id=%s"
        val = ( destination_max, destination, destination_type, ai_status, ai_start_date, ai_stop_date, ai_spend_cap, campaign_id )
        mycursor.execute(sql, val)
        mydb.commit()
        mycursor.close()
        mydb.close()
        return True

def check_initial_bid(keyword_id, df):
    mydb = connectDB(DATABASE)
    adgroup_id = df['adgroup_id'].iloc[0]
    df_check = pd.read_sql( "SELECT * FROM adgroup_initial_bid WHERE keyword_id={} AND adgroup_id={}".format(keyword_id, adgroup_id), con=mydb )
#     print(type(campaign_id.astype(dtype=object)))
    if df_check.empty:
        engine = create_engine( 'mysql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE) )
        with engine.connect() as conn, conn.begin():
            df.to_sql( "adgroup_initial_bid", conn, if_exists='append',index=False )
        engine.dispose()
    mydb.close()
    return


def insert_result( campaign_id, mydict ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = "INSERT INTO result ( campaign_id, result ) VALUES ( %s, %s )"
    val = ( campaign_id, mydict )
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
    try:
        results = str(results[0][0], encoding='utf-8')
    except:
        results = '{}'
    mycursor.close()
    mydb.close()
    return results

def check_optimal_weight(campaign_id, df):
    mydb = connectDB(DATABASE)
    df_check = pd.read_sql( "SELECT * FROM optimal_weight WHERE campaign_id={}".format(campaign_id), con=mydb )
    if df_check.empty:
        engine = create_engine( 'mysql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE) )
        with engine.connect() as conn, conn.begin():
            df.to_sql( "optimal_weight", conn, if_exists='append',index=False )
        engine.dispose()
    else:
        mycursor = mydb.cursor()
        df.drop(['campaign_id'], axis=1)
        for column in df.columns:
            try:
                sql = ("UPDATE optimal_weight SET {}='{}' WHERE campaign_id={}".format( column, df[column].iloc[0], campaign_id))
                mycursor.execute(sql)
                mydb.commit()
            except Exception as e:
                print('[gdn_db.check_optimal_weight]: ', e)
                pass
        mycursor.close()
    mydb.close()
    return

def update_init_bid(campaign_id=None, keyword_id=None, ad_group_id=None, bid_ratio=None):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    if campaign_id and bid_ratio:
        sql = "SELECT bid_amount FROM adgroup_initial_bid WHERE {}={}".format('campaign_id', campaign_id)
        update_sql = "UPDATE adgroup_initial_bid SET bid_amount={} WHERE {}={}".format('{}', 'campaign_id', campaign_id)
    elif keyword_id and ad_group_id and bid_ratio:
        sql = "SELECT bid_amount FROM adgroup_initial_bid WHERE {}={} AND {}={}".format('keyword_id', keyword_id, 'adgroup_id', ad_group_id)
        update_sql = "UPDATE adgroup_initial_bid SET bid_amount={} WHERE {}={} AND {}={}".format('{}', 'keyword_id', keyword_id, 'adgroup_id', ad_group_id)
    else:
        mycursor.close()
        mydb.close()
        if not campaign_id or not keyword_id:
            print('[update_init_bid]: campaign_id or keyword_id not set.')
        elif not bid_ratio:
            print('[update_init_bid]: bid_ratio not set.')
        return
    ### select
    mycursor.execute(sql)
    init_bid = mycursor.fetchall()
    init_bid = int(init_bid[0][0])
    init_bid = init_bid * bid_ratio
    ### update
    update_sql = update_sql.format(init_bid)
    mycursor.execute(update_sql)
    mydb.commit()
    mycursor.close()
    mydb.close()    
    return

def get_current_init_bid(campaign_id=None, keyword_id=None, ad_group_id=None):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    ### select
    if campaign_id:
        sql = "SELECT bid_amount FROM adgroup_initial_bid WHERE {column}={value}".format(column='campaign_id', value=campaign_id)
    elif keyword_id and ad_group_id:
        sql = "SELECT bid_amount FROM adgroup_initial_bid WHERE {}={} AND {}={}".format('keyword_id', keyword_id, 'adgroup_id', ad_group_id)
    else:
        mycursor.close()
        mydb.close()
        if not campaign_id or not keyword_id:
            print('[update_init_bid]: campaign_id or keyword_id not set.')
        elif not bid_ratio:
            print('[update_init_bid]: bid_ratio not set.')
        return
    mycursor.execute(sql)
    init_bid = mycursor.fetchall()
    init_bid = init_bid[0][0]
    mydb.close()
    mycursor.close()
    return init_bid
    
def get_campaign_ai_brief( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql =  "SELECT destination, destination_type, ai_spend_cap, ai_start_date, ai_stop_date FROM campaign_target WHERE campaign_id={}".format(campaign_id)
    mycursor.execute(sql)
    field_name = [field[0] for field in mycursor.description]
    values = mycursor.fetchone()
    row = dict(zip(field_name, values))
    mycursor.close()
    mydb.close()
    return row


# In[9]:


#!jupyter nbconvert --to script gsn_db.ipynb


# In[ ]:




