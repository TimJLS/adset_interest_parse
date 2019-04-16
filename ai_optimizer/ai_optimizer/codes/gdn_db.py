#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
import pandas as pd
import datetime
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

DATABASE="dev_gdn"

def connectDB(db_name):
    mydb = mysql.connector.connect(
        host="aws-dev-ai-private.adgeek.cc",
        user="app",
        passwd="adgeek1234",
        database=db_name
    )
    return mydb

def into_table(df, table=None):
    engine = create_engine( 'mysql://app:adgeek1234@aws-dev-ai-private.adgeek.cc/{}'.format(DATABASE) )
    with engine.connect() as conn, conn.begin():
        df.to_sql(table, conn, if_exists='append',index=False)
        engine.dispose()

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
            print(e)
            pass
    mycursor.close()
    mydb.close()
    return

def get_campaign(campaign_id=None):
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now()
    if campaign_id is None:
        df = pd.read_sql( "SELECT * FROM campaign_target", con=mydb )
        return df
    else:
        df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id='{}'".format(campaign_id), con=mydb )
        df_camp = pd.DataFrame(columns=df.columns)
        stop_time = df['stop_time'][df.campaign_id==campaign_id].iloc[0]
        if stop_time >= request_time:
            df_camp= pd.concat( [ df_camp, df[df.campaign_id==campaign_id] ], axis=0 )
        mydb.close()
        return df_camp
    
def check_campaignid_target(account_id, campaign_id, destination, destination_type):
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id='{}'".format(campaign_id), con=mydb )
    mycursor = mydb.cursor()
    if df.empty:
        sql = "INSERT INTO campaign_target ( customer_id, campaign_id, destination, destination_type ) VALUES ( %s, %s, %s, %s )"
        val = ( account_id, campaign_id, destination, destination_type )
        mycursor.execute(sql, val)
        mydb.commit()
        mycursor.close()
        mydb.close()
        return False
    else:
        sql = "UPDATE campaign_target SET destination=%s, destination_type=%s WHERE campaign_id=%s"
        val = ( destination, destination_type, campaign_id )
        mycursor.execute(sql, val)
        mydb.commit()
        mycursor.close()
        mydb.close()
        return True

def check_initial_bid(adgroup_id, df):
    mydb = connectDB(DATABASE)
    df_check = pd.read_sql( "SELECT * FROM adgroup_initial_bid WHERE adgroup_id={}".format(adgroup_id), con=mydb )
#     print(type(campaign_id.astype(dtype=object)))
    if df_check.empty:
        engine = create_engine( 'mysql://app:adgeek1234@aws-dev-ai-private.adgeek.cc/{}'.format(DATABASE) )
        with engine.connect() as conn, conn.begin():
            df.to_sql( "adgroup_initial_bid", conn, if_exists='append',index=False )
        engine.dispose()
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
    results = str(results[0][0], encoding='utf-8')
    mycursor.close()
    mydb.close()
    return results

def check_optimal_weight(campaign_id, df):
    mydb = connectDB(DATABASE)
    df_check = pd.read_sql( "SELECT * FROM optimal_weight WHERE campaign_id={}".format(campaign_id), con=mydb )
    if df_check.empty:
        engine = create_engine( 'mysql://app:adgeek1234@aws-dev-ai-private.adgeek.cc/{}'.format(DATABASE) )
        with engine.connect() as conn, conn.begin():
            df.to_sql( "optimal_weight", conn, if_exists='append',index=False )
        engine.dispose()
        return
    else:
        mycursor = mydb.cursor()
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


# In[2]:





# In[ ]:




