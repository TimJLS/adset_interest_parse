#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import datetime
import pandas as pd
import time

import csv
from io import StringIO
import os
PATH = 'data/brightroll/'


# In[3]:


CLIENT_ID = 'dj0yJmk9UGd2aWZOUjBBeUhhJnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTdh'
AUTH_CODE = 'egp83fb'
ENCODED_CLIENT_SECRET = 'ZGoweUptazlVR2QyYVdaT1VqQkJlVWhoSm5NOVkyOXVjM1Z0WlhKelpXTnlaWFFtYzNZOU1DWjRQVGRoOjNlZWZmMmYxMTZiYzQ2ODgwMjQ3MTcxYmUxOTBhZTYxN2U2MDA0YzU='


# In[4]:


def get_access_token():
    url = 'https://api.login.yahoo.com/oauth2/get_token'
    headers = {
        "Content-Type":"application/x-www-form-urlencoded",
        "Authorization": "Basic {}".format(ENCODED_CLIENT_SECRET)
    }
    init_payload = {
    "code":"{}".format(AUTH_CODE),
    "redirect_uri":"oob",
    "grant_type":"authorization_code"
    }
    r = requests.post(url, headers=headers, data=init_payload)
    try:
        json.loads(r.text)['access_token']
        global response
        response = json.loads(r.text)
        return access_token
    except:
        access_token = refresh_access_token(response)
        return access_token
    


# In[5]:


def refresh_access_token(response):
    refresh_token = response['refresh_token']
    url = 'https://api.login.yahoo.com/oauth2/get_token'
    headers = {
        "Content-Type":"application/x-www-form-urlencoded",
        "Authorization": "Basic {}".format(ENCODED_CLIENT_SECRET)
    }
    refresh_payload = {
    "refresh_token":"{}".format(refresh_token),
    "redirect_uri":"oob",
    "grant_type":"refresh_token"
    }
    r = requests.post(url, headers=headers, data=refresh_payload)
    global access_token
    access_token = json.loads(r.text)['access_token']
    return access_token


# In[6]:


get_access_token()


# In[7]:


class DatePreset:
    today = 'today'
    lifetime = 'lifetime'

class BrightrollField:
    response = 'response'
    id = 'id'
    order_id = 'Order Id'
    line_id = 'Line Id'
    account_id = 'accountId'
    budget_schedules = 'budgetSchedules'
    budget_schedule = 'budgetSchedule'
    daily_budget = 'dailyBudget'
    budget = 'budget'
    goal_type = 'goalType'
    status = 'status'
    goal_value = 'goalValue'
    schedule_dailyBudget = 'scheduleDailyBudget'
    schedule_budget = 'scheduleBudget'
    schedule_budget_type = 'scheduleBudgetType'
    start_date = 'startDate'
    end_date = 'endDate'
    errors = 'errors'
    bid_price = 'bidPrice'
    billing_price = 'billingPrice'
    max_goal = 'maxGoal'
    goal_amount = 'goalAmount'
    advertiser_spending = 'Advertiser Spending Campaign Currency'
    cpc = 'CPC Campaign Currency'
    clicks = 'Clicks'
    impressions = 'Impressions'
    conversion = 'Conversion'
    order_currency_id = 'Order Currency Id'
    order_currency = 'Order Currency'
    media_type_id = 'Media Type Id'
    media_type = 'Media Type'
    cpa = 'CPA Campaign Currency'
    ctr = 'CTR'
    ecpm = 'Advertiser eCPM Campaign Currency'
    date_type = 'Day'
    line = 'Line'
    order = 'Order'
    
class AdgeekField:
    campaign_id = 'campaign_id'
    line_id = 'line_id'
    spend_cap = 'spend_cap'
    start_time = 'start_time'
    stop_time = 'stop_time'
    goal_type = 'target_type'
    goal_value = 'goal_value'
    period = 'period'
    ioid = 'ioid'
    cost_per_impressions = 'cost_per_impressions'
    cost_per_target = 'cost_per_target'
    daily_budget = 'daily_budget'
    spend = 'spend'
    target = 'target'
    bid_amount = 'bid_amount'
    budget = 'budget'
    request_time = 'request_time'
    daily_charge = 'daily_charge'
    target_left = 'target_left'
    destination = 'destination'
    billing_price = 'billing_price'
    max_goal = 'max_goal'
    impressions = 'impressions'
    media_type ='media_type'
    cost_per_click = 'cost_per_click'
    cost_per_action = 'cost_per_action'
    ctr = 'ctr'
    clicks = 'clicks'


# In[289]:


class Campaigns(object):
    def __init__(self, campaign_id):
        self.campaign_id = campaign_id
        self.spend_cap=0
        self.campaign_features = dict()
        self.campaign_insights = dict( ( [AdgeekField.cost_per_target, 0], [AdgeekField.target, 0] ) )
        self.headers = {
            "X-Auth-Method":"OAUTH",
            "Content-Type":"application/json",
            "X-Auth-Token": "{}".format(access_token)
        }
    def retrieve_features(self):
        url = 'https://dspapi.admanagerplus.yahoo.com/traffic/campaigns/{}'.format(self.campaign_id)
        r = requests.get( url, headers=self.headers )
        data = json.loads(r.text)[ BrightrollField.response ]
        try:
            budget_schedules = data[ BrightrollField.budget_schedules ]
            for sch in budget_schedules:
                self.spend_cap += sch[ BrightrollField.schedule_budget ]
            self.start_time = datetime.datetime.strptime( budget_schedules[0][ BrightrollField.start_date ],'%Y-%m-%dT%H:%M:%Sz' ) + datetime.timedelta(hours=8)
            self.stop_time = datetime.datetime.strptime( budget_schedules[-1][ BrightrollField.end_date ],'%Y-%m-%dT%H:%M:%Sz' ) + datetime.timedelta(hours=8)

        except:
            self.start_time = datetime.datetime.strptime( data['campaignStartDate'],'%Y-%m-%dT%H:%M:%SZ' ) + datetime.timedelta(hours=8)
            self.stop_time = datetime.datetime.strptime( data['campaignEndDate'],'%Y-%m-%dT%H:%M:%SZ' ) + datetime.timedelta(hours=8)
            self.spend_cap = data[ BrightrollField.budget ]

        self.goal_type = data[ BrightrollField.goal_type ]
        self.goal_value = data[ BrightrollField.goal_value ]
        self.period = ( self.stop_time - self.start_time ).days + 1
        self.campaign_features.update( {AdgeekField.spend_cap:self.spend_cap})
        self.campaign_features.update( {AdgeekField.start_time:self.start_time})
        self.campaign_features.update( {AdgeekField.stop_time:self.stop_time})
        self.campaign_features.update( {AdgeekField.goal_type:self.goal_type})
        self.campaign_features.update( {AdgeekField.goal_value:self.goal_value})
        self.campaign_features.update( {AdgeekField.period:self.period})
        return self.campaign_features
    
    def retrieve_lifetime_performance(self, date_preset = DatePreset.lifetime):
        url = 'http://api-sched-v3.admanagerplus.yahoo.com/yamplus_api/extreport/'
        try:
            start_time = self.start_time.strftime( '%Y-%m-%dT%H:%M:%S+{}'.format('08:00') )
            stop_time = self.stop_time.strftime( '%Y-%m-%dT%H:%M:%S+{}'.format('08:00') )
        except:
            self.retrieve_features()
            start_time = self.start_time.strftime( '%Y-%m-%dT%H:%M:%S+{}'.format('08:00') )
            stop_time = self.stop_time.strftime( '%Y-%m-%dT%H:%M:%S+{}'.format('08:00') )
        if date_preset == DatePreset.lifetime:
            date_type_id = 11
            interval_type_id = 1
        elif date_preset == DatePreset.today:
            date_type_id = 1
            interval_type_id = 2
        body = {
            "reportOption": {
                "timezone": "Asia/Shanghai",
                "currency": 3,
                "dimensionTypeIds": [ 5, 63 ],
                "metricTypeIds": [ 44, 1, 2, 23, 11, 41, 43, 45 ]
            },
            "intervalTypeId": interval_type_id,
            "dateTypeId": date_type_id,
            "startDate": "{}".format(start_time),
            "endDate": "{}".format(stop_time)
        }
        r = requests.post( url, headers=self.headers, data="{}".format(json.dumps(body)) )
        try:
            customer_report_id = json.loads(r.text)['customerReportId']
        except:
            self.retrieve_lifetime_performance(date_preset=date_preset)
        url = url + customer_report_id
        time.sleep(10)
        r = requests.get( url, headers=self.headers )
        while json.loads(r.text)['status'] != 'Success':
            time.sleep(19)
            r = requests.get( url, headers=self.headers )
        url = json.loads(r.text)['url']
        r = requests.get( url )
        reader = csv.DictReader( StringIO(r.text) )
        json_data = json.loads( json.dumps( list( reader ) ) )
        for d in json_data:
            if d['Order Id']==str( self.campaign_id ):
                self.campaign_insights.update( {AdgeekField.spend:float( d['Advertiser Spending Campaign Currency'] )} )
                self.campaign_insights.update( {AdgeekField.cost_per_target:float( d['CPC Campaign Currency'] )} )
                self.campaign_insights.update( {AdgeekField.target:int( d['Clicks'] )} )
                self.campaign_insights.update( {AdgeekField.impressions:int( d['Impressions'] )} )
        return self.campaign_insights
    
    def initialize(self, date_preset=DatePreset.lifetime):
        self.retrieve_features()
        self.retrieve_lifetime_performance(date_preset=date_preset)
        self.campaign_insights.update( {AdgeekField.campaign_id:self.campaign_id} )
        self.campaign = { **self.campaign_insights, **self.campaign_features }
        return self.campaign


# In[210]:


class Lines(Campaigns):
    def __init__(self, line_id):
        self.line_id = line_id
        self.headers = {
            "X-Auth-Method":"OAUTH",
            "Content-Type":"application/json",
            "X-Auth-Token": "{}".format(access_token)
        }
        self.line_features = dict()
        self.line_insights = dict( ( [AdgeekField.cost_per_target, 0], [AdgeekField.target, 0] ) )
        
    def retrieve_features(self):
        url = 'https://dspapi.admanagerplus.yahoo.com/traffic/lines/{}'.format(self.line_id)
        r = requests.get( url, headers=self.headers )
        data = json.loads(r.text)[ BrightrollField.response ]
        budget_schedule = data[ BrightrollField.budget_schedule ]
        self.budget = data[ BrightrollField.budget_schedule ][ BrightrollField.budget ]
        self.daily_budget = data[ BrightrollField.daily_budget ]
        self.bid_amount = data[ BrightrollField.bid_price ]
        self.billing_price = data[ BrightrollField.billing_price ]
        self.max_goal = data[ BrightrollField.max_goal ]
        self.goal_amount = data[ BrightrollField.goal_amount ]
        self.line_features.update( {AdgeekField.budget:self.budget} )
        self.line_features.update( {AdgeekField.daily_budget:self.daily_budget} )
        self.line_features.update( {AdgeekField.bid_amount:self.bid_amount} )
        self.line_features.update( {AdgeekField.billing_price:self.billing_price} )
        self.line_features.update( {AdgeekField.max_goal:self.goal_amount} )
        return self.line_features
    
    def retrieve_performance(self, date_preset = DatePreset.today):
        url = 'http://api-sched-v3.admanagerplus.yahoo.com/yamplus_api/extreport/'
        request_time = datetime.datetime.now().strftime( '%Y-%m-%dT%H:%M:%S+{}'.format('08:00') )
        if date_preset == DatePreset.lifetime:
            date_type_id = 11
            interval_type_id = 1
        elif date_preset == DatePreset.today:
            date_type_id = 1
            interval_type_id = 2
        body = {
            "reportOption": {
                "timezone": "Asia/Shanghai",
                "currency": 3,
                "dimensionTypeIds": [ 5, 6, 63 ],
                "metricTypeIds": [ 44, 1, 2, 23, 11, 41, 43, 45 ]
            },
            "intervalTypeId": interval_type_id,
            "dateTypeId": date_type_id,
        }
        r = requests.post( url, headers=self.headers, data="{}".format(json.dumps(body)) )
        customer_report_id = json.loads(r.text)['customerReportId']
        url = url + customer_report_id
        time.sleep(3)
        r = requests.get( url, headers=self.headers )
        while json.loads(r.text)['status'] != 'Success':
            time.sleep(7)
            r = requests.get( url, headers=self.headers )
        url = json.loads(r.text)['url']
        print(url)
        r = requests.get( url )
        reader = csv.DictReader( StringIO(r.text) )
        json_data = json.loads( json.dumps( list( reader ) ) )
        for d in json_data:
            if d['Line Id']==str( self.line_id ):
                self.line_insights.update( {AdgeekField.spend:float( d['Advertiser Spending Campaign Currency'] )} )
                self.line_insights.update( {AdgeekField.cost_per_target:float( d['CPC Campaign Currency'] )} )
                self.line_insights.update( {AdgeekField.target:int( d['Clicks'] )} )
                self.line_insights.update( {AdgeekField.impressions:int( d['Impressions'] )} )
        return self.line_insights
    
    def initialize(self, date_preset=DatePreset.today):
        self.retrieve_features()
#         self.retrieve_performance(date_preset=date_preset)
        self.line_insights.update( {AdgeekField.line_id:self.line_id} )
        self.line = { **self.line_insights, **self.line_features }
        return self.line


# In[168]:


campaign_id = 154063
campaign_id = 94982
total_clicks = 1000000


# In[45]:


def get_line_insights(campaign_id):
    request_time = datetime.datetime.now()
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    df = pd.read_sql( "SELECT * FROM line_insights WHERE campaign_id={}".format(campaign_id) , con=mydb )
    line_list = df['line_id'].unique()
    return df


# In[184]:


def retrieve_real_time_performance(category='campaign'):
    url = 'http://api-sched-v3.admanagerplus.yahoo.com/yamplus_api/extreport/'
    headers = {
            "X-Auth-Method":"OAUTH",
            "Content-Type":"application/json",
            "X-Auth-Token": "{}".format(access_token)
    }
    column_names = {
        BrightrollField.order_id : AdgeekField.campaign_id,
        BrightrollField.impressions : AdgeekField.impressions,
        BrightrollField.clicks : AdgeekField.clicks,
        BrightrollField.media_type : AdgeekField.media_type,
        BrightrollField.cpc : AdgeekField.cost_per_click,
        BrightrollField.cpa : AdgeekField.cost_per_action,
        BrightrollField.advertiser_spending : AdgeekField.spend,
        BrightrollField.ctr : AdgeekField.ctr,
        BrightrollField.ecpm : AdgeekField.cost_per_impressions
    }
    request_time = datetime.datetime.now().strftime( '%Y-%m-%dT%H:%M:%S+{}'.format('08:00') )
    date_type_id = 1
    interval_type_id = 2
    if category == 'campaign':
        body = {
            "reportOption": {
                "timezone": "Asia/Shanghai",
                "currency": 3,
                "dimensionTypeIds": [ 5, 63 ],
                "metricTypeIds": [ 44, 1, 2, 23, 11, 41, 43, 45 ]
            },
            "intervalTypeId": interval_type_id,
            "dateTypeId": date_type_id,
        }
    elif category == 'line':
        body = {
            "reportOption": {
                "timezone": "Asia/Shanghai",
                "currency": 3,
                "dimensionTypeIds": [ 5, 6, 63 ],
                "metricTypeIds": [ 44, 1, 2, 23, 11, 41, 43, 45 ]
            },
            "intervalTypeId": interval_type_id,
            "dateTypeId": date_type_id,
        }
        column_names.update( {BrightrollField.line_id : AdgeekField.line_id} )
    r = requests.post( url, headers=headers, data="{}".format(json.dumps(body)) )
    customer_report_id = json.loads(r.text)['customerReportId']
    url = url + customer_report_id
    time.sleep(10)
    r = requests.get( url, headers=headers )
    while json.loads(r.text)['status'] != 'Success':
        time.sleep(20)
        r = requests.get( url, headers=headers )
        print(json.loads(r.text))
    url = json.loads(r.text)['url']
    print(url)
    r = requests.get( url )
    reader = csv.DictReader( StringIO(r.text) )
    json_data = json.loads( json.dumps( list( reader ) ) )
    df = pd.DataFrame.from_records( json_data )
    df.loc[df[ BrightrollField.cpa ] == 'N/A',  BrightrollField.cpa ] = 0
    df = df.apply( pd.to_numeric, errors='ignore' )
    df = df[ list(column_names.keys()) ]
    df = df.rename( columns=column_names )
    df = df[ (df.cost_per_click != 'N/A') ]
    df.drop( df.tail(1).index, inplace=True )
    df[AdgeekField.request_time]=datetime.datetime.now()
    if category == 'campaign':
        insertion('campaign_insights', df)
    elif category == 'line':
        insertion('line_insights', df)
    return df


# In[283]:


import mysql.connector
import pandas as pd
import datetime
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

DATABASE="Brightroll"

def connectDB(db_name):
    mydb = mysql.connector.connect(
        host="localhost",
        user="app",
        passwd="adgeek1234",
        database=db_name
    )
    return mydb

def insertion(table, df):
    engine = create_engine( 'mysql://app:adgeek1234@localhost/{}'.format(DATABASE) )
    with engine.connect() as conn, conn.begin():
        df.to_sql(table, conn, if_exists='append',index=False)

def truncate_realtime_table(table):
    engine = create_engine( 'mysql://root:adgeek1234@localhost/{}'.format(DATABASE) )
    with engine.connect() as conn, conn.begin():
        engine.execute("truncate table {}".format(table))
    return

def update_campaign_target(df_camp):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql = ("update campaign_target set cost_per_target=%s, daily_charge=%s, destination=%s, goal_value=%s, impressions=%s, period=%s, spend=%s, spend_cap=%s, target=%s, target_left=%s, target_type=%s, start_time=%s, stop_time=%s where campaign_id=%s")
    val = (
        df_camp['cost_per_target'].iloc[0].astype(dtype=object),
        df_camp['daily_charge'].iloc[0].astype(dtype=object),
        df_camp['destination'].iloc[0].astype(dtype=object),
        df_camp['goal_value'].iloc[0].astype(dtype=object),
        df_camp['impressions'].iloc[0].astype(dtype=object),
        df_camp['period'].iloc[0].astype(dtype=object),
        df_camp['spend'].iloc[0].astype(dtype=object),
        df_camp['spend_cap'].iloc[0].astype(dtype=object),
        df_camp['target'].iloc[0].astype(dtype=object),
        df_camp['target_left'].iloc[0].astype(dtype=object),
        df_camp['target_type'].iloc[0],
        df_camp['start_time'].iloc[0].isoformat(),
        df_camp['stop_time'].iloc[0].isoformat(),
        df_camp['campaign_id'].iloc[0].astype(dtype=object)
    )
    mycursor.execute(sql, val)
    mydb.commit()
    return


# In[287]:


def data_collect(campaign_id, total_clicks):
    get_access_token()
    truncate_realtime_table("campaign_insights")
    truncate_realtime_table("line_insights")
    df_camp = retrieve_real_time_performance(category='campaign')
    insertion("campaign_insights", df_camp)
    time.sleep(20)
    df_line = retrieve_real_time_performance(category='line')
    insertion("line_insights", df_line)
    time.sleep(10)
    camp = Campaigns(campaign_id=campaign_id)
    camp.initialize(date_preset=DatePreset.lifetime)
    charge = camp.campaign[ AdgeekField.target ]
    target_left = { AdgeekField.target_left: int(total_clicks) - int(charge) }
    daily_charge = { AdgeekField.daily_charge: int(total_clicks) / int(camp.period) }
    request_dict = { AdgeekField.request_time: datetime.datetime.now() }
    target_dict = { AdgeekField.destination: total_clicks }
    campaign_dict = {
        **camp.campaign,
        **target_left,
        **target_dict,
        **daily_charge,
        **request_dict
    }
    df_camp = pd.DataFrame(campaign_dict, index=[0])
    update_campaign_target(df_camp)
    # brightroll_db.
    df_line_insights = get_line_insights(campaign_id)
    df_line_insights = df_line_insights[df_line_insights.request_time.dt.date==datetime.datetime.now().date()]
    df_line_insights = df_line_insights.drop(columns=['request_time'])
    for line_id in df_line_insights['line_id'].unique():
        line = Lines(line_id=int(line_id))
        time.sleep(10)
        line.initialize()
        df_line_insights = df_line_insights.apply( pd.to_numeric, errors='ignore' )
        df_line_insights[df_line_insights.line_id==line_id] = df_line_insights[df_line_insights.line_id==line_id].fillna(value=line.line)
        df_line_insights['request_time'] = datetime.datetime.now()
    insertion("line_insights", df_line_insights)
    return


# In[288]:


data_collect(campaign_id, total_clicks)

