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


# In[2]:


def connectDB(db_name):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="adgeek1234",
        database=db_name
    )
    return mydb
#connectDB("ad_activity")


# In[3]:


def selectData(table):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM %s" %(table))
    results = mycursor.fetchall()
    
#    for record in results:
#        print(record)
#        print("%s, %s, %s" %(col1, col2, col3))


# In[4]:


def insertData(ad_id,cpc,clicks,impressions,reach,bid_amount):
    mydb = connectDB("ad_activity")
    mycursor = mydb.cursor()
    sql = "INSERT INTO all_time (ad_id,cpc,clicks,impressions,reach,bid_amount,request_time) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (ad_id,cpc,clicks,impressions,reach,bid_amount,datetime.datetime.now())
    mycursor.execute(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "record inserted")
#insertData(19910630, 5.0, 10, 100, 99, 12)


# In[5]:


def getData(ad_id):
    mydb = connectDB("ad_activity")
    
    df = pd.read_sql("SELECT * FROM all_time WHERE ad_id=%s"%(ad_id), con=mydb)
    return df
#getData(19910630)


# In[6]:


def makeDF(ad_id,cpc,clicks,impressions,reach,bid_amount):
    d={'ad_id':[ad_id],'request_time':[datetime.datetime.now()],'cpc':[cpc],'clicks':[clicks],
       'impressions':[impressions],'reach':[reach],'bid_amount':[bid_amount],'0':[1],'1':[1],'2':[1],'3':[1],'4':[1],
      '5':[1],'6':[1],'7':[1],'8':[1],'9':[1],'10':[1]}
    df = pd.DataFrame(data=d)
    return df
#makeDF(19910630, 5.0, 10, 100, 99, 12)


# In[7]:


def intoDB(table,hour_data):
    engine = create_engine('mysql://root:adgeek1234@localhost/ad_activity')
    with engine.connect() as conn, conn.begin():
        df.to_sql("all_time", conn, if_exists='append',index=False)
    return df

