#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path

import facebook_business.adobjects.adaccount as adaccount
import facebook_business.adobjects.adset as adset
import facebook_business.adobjects.ad as ad
import facebook_business.adobjects.campaign as campaign
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adactivity import AdActivity
from facebook_business.adobjects.insightsresult import InsightsResult
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_datacollector import Field
from facebook_datacollector import DatePreset
import mysql_adactivity_save as mysql_saver

import adgeek_permission as permission
permission.init_facebook_api()

import json
import datetime
import pandas as pd
from bid_operator import *
import math
import collections


# In[2]:


CONVERSION_METRICS = {
    'landing_page_view': 'landing_page_view',
    'link_click': 'link_click'
}
CONVERSION_KEYS = [ "purchase", "add_to_cart", "view_content" ]
CAMPAIGN_OBJECTIVE = {
    'LINK_CLICKS': 'link_click',
    'POST_ENGAGEMENT': 'post_engagement', 
    'VIDEO_VIEWS': 'video_view', 
    'CONVERSIONS':'offsite_conversion.fb_pixel_purchase',
    'ADD_TO_CART':'offsite_conversion.fb_pixel_add_to_cart',
    'APP_INSTALLS': 'app_installs',
    'BRAND_AWARENESS': 'brand_awareness',
    'EVENT_RESPONSES': 'event_responses',
    'LEAD_GENERATION': 'leadgen.other',
    'LOCAL_AWARENESS': 'local_awareness',
    'MESSAGES': 'messages',
    'OFFER_CLAIMS': 'offer_claims',
    'PAGE_LIKES': 'likes',
    'PRODUCT_CATALOG_SALES': 'product_catalog_sales',
    'REACH': 'reach',
    'ALL_CLICKS': 'clicks',
}
CAMPAIGN_FEATURE = {
    'spend_cap': campaign.Campaign.Field.spend_cap,
    'objective': campaign.Campaign.Field.objective,
    'start_time': campaign.Campaign.Field.start_time,
    'stop_time': campaign.Campaign.Field.stop_time,
}
ADSET_FEATURE = {
    'bid_amount': adset.AdSet.Field.bid_amount,
    'daily_budget': adset.AdSet.Field.daily_budget
}

GENERAL_INSIGHTS = {
    'impressions': AdsInsights.Field.impressions,
    'reach': AdsInsights.Field.reach,
    'spend': AdsInsights.Field.spend,
    'cpc': AdsInsights.Field.cpc,
    'clicks': AdsInsights.Field.clicks,
    'ctr': AdsInsights.Field.ctr
}
TARGET_INSIGHTS = {
    'actions': AdsInsights.Field.actions,
    'cost_per_actions': AdsInsights.Field.cost_per_action_type,
}


# In[3]:


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
DATABASE="dev_facebook_test"
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

def get_campaign_ai_brief( campaign_id ):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    sql =  "SELECT ai_spend_cap, ai_start_date, ai_stop_date, charge_type, custom_conversion_id FROM campaign_target WHERE campaign_id={}".format(campaign_id)
    mycursor.execute(sql)
    field_name = [field[0] for field in mycursor.description]
    values = mycursor.fetchone()
    row = dict(zip(field_name, values))
    mycursor.close()
    mydb.close()
    return row

def insertion(table, df):
    engine = create_engine( 'mysql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE) )
    with engine.connect() as conn, conn.begin():
        df.to_sql(table, conn, if_exists='append',index=False)
        engine.dispose()

def update_campaign_target(df_camp):
    mydb = connectDB(DATABASE)
    mycursor = mydb.cursor()
    campaign_id = df_camp['campaign_id'].iloc[0]
    df_camp.drop(['campaign_id'], axis=1)
    for column in df_camp.columns:
        try:
            sql = ("UPDATE campaign_target SET {} = '{}' WHERE campaign_id={}".format(column, df_camp[column].iloc[0], campaign_id))
            mycursor.execute(sql)
            mydb.commit()
        except Exception as e:
            print('[index_collector_conversion_facebook.update_table]: ', e)
            pass
    mycursor.close()
    mydb.close()
    return

def check_conv_metrics(campaign_id, campaign_conv_metrics):
    print('[check_conv_metrics] campaign_conv_metrics', campaign_conv_metrics)
    mydb = connectDB(DATABASE)
    df = pd.read_sql( "SELECT * FROM campaign_conversion_metrics WHERE campaign_id={}".format(campaign_id), con=mydb )
    df_camp = pd.DataFrame(campaign_conv_metrics, index=[0])
    if df.empty:
        engine = create_engine( 'mysql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE) )
        with engine.connect() as conn, conn.begin():
            df_camp.to_sql("campaign_conversion_metrics", conn, if_exists='append',index=False)
            engine.dispose()
        mydb.close()
        return False
    else:
        df_camp.drop(['campaign_id'], axis=1)
        mycursor = mydb.cursor()
        for column in df_camp.columns:
            try:
                sql = ("UPDATE campaign_conversion_metrics SET `{}` = '{}' WHERE campaign_id={}".format(column, df_camp[column].iloc[0], campaign_id))
                mycursor.execute(sql)
                mydb.commit()
            except Exception as e:
                print('[gdn_db.update_table]: ', e)
                pass
        mycursor.close()
        mydb.close()
        return True


# In[4]:


class Campaigns(object):
    def __init__( self, campaign_id ):
        self.campaign_id = campaign_id
        self.campaign_insights = dict()
        self.campaign_features = dict()
        self.info = dict()
        brief_dict = get_campaign_ai_brief( self.campaign_id )
        self.ai_spend_cap = brief_dict[Field.ai_spend_cap]
        self.ai_start_date = brief_dict[Field.ai_start_date]
        self.ai_stop_date = brief_dict[Field.ai_stop_date]
        self.charge_type = brief_dict[Field.charge_type]
        self.custom_conversion_id = brief_dict["custom_conversion_id"]

    # Getters
    
    def get_campaign_features( self ):
        ad_campaign = campaign.Campaign( self.campaign_id )
        adcamps = ad_campaign.remote_read( fields=list(CAMPAIGN_FEATURE.values()) )
        for field in list(adcamps.keys()):
            self.campaign_features.update( {field:adcamps.get(field)} )
        return self.campaign_features
        
    def get_campaign_insights( self, date_preset=None ):
        self.optimization_goal = 'offsite_conversion.custom.'+self.custom_conversion_id
        self.temp_campaign_insights = dict()
        campaigns = campaign.Campaign( self.campaign_id )
        if date_preset is None or date_preset == DatePreset.lifetime:
            params = {
                'time_range[since]': self.ai_start_date,
                'time_range[until]': self.ai_stop_date,
            }
        else:
            params = {
                'date_preset': date_preset,
            }
        try:
            insights = campaigns.get_insights(
                params=params,
                fields=list( GENERAL_INSIGHTS.values() )+list( TARGET_INSIGHTS.values() )
            )

        except:
            insights = campaigns.get_insights(
                fields=list( GENERAL_INSIGHTS.values() )+list( TARGET_INSIGHTS.values() )
            )         
            
        if len(insights) > 0:
            current_campaign = insights[0]
            if current_campaign.get(Field.impressions) and current_campaign.get(Field.spend):
                spend = current_campaign.get(Field.spend)
                impressions = current_campaign.get(Field.impressions)
                landing_page_view = current_campaign.get( "landing_page_view" ) if current_campaign.get( "landing_page_view" ) else 0
                link_click = current_campaign.get( "link_click" ) if current_campaign.get( "link_click" ) else 0
                self.campaign_insights.update({ 
                    "spend": float(spend),
                    "impressions": int(impressions),
                    "landing_page_view": int(landing_page_view),
                    "link_click": int(link_click),
                    self.optimization_goal: 0,
                    })
                self.temp_campaign_insights.update( { self.optimization_goal: 0 } )
                if current_campaign.get( Field.actions ):
                    for act in current_campaign.get( Field.actions ):
                        if 'offsite_conversion' in act["action_type"] or act["action_type"] in CONVERSION_METRICS:
                            self.temp_campaign_insights.update({act["action_type"]: int(act["value"])})
                for key, val in self.temp_campaign_insights.copy().items():
                    if val < self.temp_campaign_insights[self.optimization_goal]:
                        self.temp_campaign_insights.pop(key)
                for key, val in self.temp_campaign_insights.items():
                    if val in list(sorted(self.temp_campaign_insights.values(), reverse=True))[-3:]:
                        self.campaign_insights.update( { key: val } )
                    elif key in CONVERSION_METRICS:
                        self.campaign_insights.update( { key: val } )
                for idx, key in enumerate([key for key in self.campaign_insights.keys() if 'offsite_conversion' in key]):
                    self.campaign_insights[ CONVERSION_KEYS[idx] ] = self.campaign_insights[key]
                    del self.campaign_insights[key]
            return self.campaign_insights
    
    def get_adsets( self ):
        adset_list=list()
        campaigns = campaign.Campaign( self.campaign_id )
        adsets = campaigns.get_ad_sets( fields = [ adset.AdSet.Field.id ])
        ads = campaigns.get_ad_sets( fields = [ adset.AdSet.Field.id ])
        for adset_id in adsets:
            adset_list.append( adset_id.get("id") )
        return adset_list

    def get_adsets_active(self):
        adset_active_list = list()
        camp = campaign.Campaign( self.campaign_id )
        adsets = camp.get_ad_sets( fields = [adset.AdSet.Field.id ,  adset.AdSet.Field.status])
#         print('[get_adsets_active] adsets:', adsets )
        for adset_id in adsets:
            if  adset_id.get("status") == 'ACTIVE' :
                adset_active_list.append( adset_id.get("id") )
        print('[get_adsets_active] adset_active_list:', adset_active_list )
        return adset_active_list
    
    def retrieve_all(self, date_preset=None):
        self.get_campaign_features()
        self.get_campaign_insights(date_preset=date_preset)
        
        self.campaign_features[ Field.campaign_id ] = self.campaign_features.pop('id')
        self.campaign_features[ Field.target_type ] = self.campaign_features.pop('objective')
        self.campaign_features[ Field.start_time ] = datetime.datetime.strptime( self.campaign_features[Field.start_time],'%Y-%m-%dT%H:%M:%S+%f' )
        self.campaign_features[ Field.period ] = ( self.ai_stop_date - self.ai_start_date ).days + 1
        self.campaign_features[ Field.start_time ] = self.campaign_features[Field.start_time].strftime( '%Y-%m-%d %H:%M:%S' )
        self.campaign_features[ Field.daily_budget ] = int( self.ai_spend_cap )/self.campaign_features[Field.period]  
        self.info = { **self.campaign_insights, **self.campaign_features }
        return self.info


# In[5]:


class AdSets(object):
    def __init__( self, adset_id ):
        self.adset_id = adset_id
        self.adset_features = dict()
        self.adset_insights = dict()
        self.info = dict()

        adsets = adset.AdSet( self.adset_id )
        current_adset = adsets.remote_read( fields=[adset.AdSet.Field.campaign_id] )
        self.campaign_id = current_adset.get( Field.campaign_id )
        
        brief_dict = get_campaign_ai_brief( self.campaign_id )
        self.ai_spend_cap = brief_dict[Field.ai_spend_cap]
        self.ai_start_date = brief_dict[Field.ai_start_date]
        self.ai_stop_date = brief_dict[Field.ai_stop_date]
        self.charge_type = brief_dict[Field.charge_type]
        self.custom_conversion_id = brief_dict["custom_conversion_id"]
        
    # Getters
        
    def get_adset_features( self ):
        adsets = adset.AdSet( self.adset_id )
        adsets = adsets.remote_read( fields=list( ADSET_FEATURE.values() ) )
        for field in list(adsets.keys()):
            self.adset_features.update( { field:adsets.get(field) } )
        return self.adset_features
    
    def get_adset_insights( self, date_preset=None ):
        adsets = adset.AdSet( self.adset_id )
        self.optimization_goal = 'offsite_conversion.custom.'+self.custom_conversion_id
        self.temp_adset_insights = dict()
        params = {}
        if date_preset is None or date_preset == DatePreset.lifetime:
            params = {
                'time_range[since]': self.ai_start_date,
                'time_range[until]': self.ai_stop_date,
            }
        else:
            params = {
                'date_preset': date_preset,
            }
        try:
            insights = adsets.get_insights(
                params=params,
                fields=list( GENERAL_INSIGHTS.values() )+list( TARGET_INSIGHTS.values() )
            )
        except Exception as e:
            print('[AdSets.get_adset_insights] adset id: ', self.adset_id)
            print('[AdSets.get_adset_insights] error: ', e)
            return self.adset_insights
        if len(insights) > 0:
            current_adset = insights[0]
            if current_adset.get(Field.impressions):
                spend = insights[0].get( Field.spend )
                impressions = insights[0].get( Field.impressions )
                landing_page_view = insights[0].get( "landing_page_view" ) if insights[0].get( "landing_page_view" ) else 0
                link_click = insights[0].get( "link_click" ) if insights[0].get( "link_click" ) else 0
                self.adset_insights.update({ 
                    "spend": float(spend),
                    "impressions": int(impressions),
                    "landing_page_view": int(landing_page_view),
                    "link_click": int(link_click),
                    self.optimization_goal: 0,
                    })
                self.temp_adset_insights.update( { self.optimization_goal: 0 } )
                if current_adset.get( Field.actions ):
                    for act in current_adset.get( Field.actions ):
                        if 'offsite_conversion' in act["action_type"] or act["action_type"] in CONVERSION_METRICS:
                            self.temp_adset_insights.update({act["action_type"]: int(act["value"])})
                    for key, val in self.temp_adset_insights.copy().items():
                        if val < self.temp_adset_insights[self.optimization_goal]:
                            self.temp_adset_insights.pop(key)
                    for key, val in self.temp_adset_insights.items():
                        if val in list(sorted(self.temp_adset_insights.values(), reverse=True))[-3:]:
                            self.adset_insights.update( { key: val } )
                        elif key in CONVERSION_METRICS:
                            self.adset_insights.update( { key: val } )
                    for idx, key in enumerate([key for key in self.adset_insights.keys() if 'offsite_conversion' in key]):
                        try:
                            self.adset_insights[ CONVERSION_KEYS[idx] ] = self.adset_insights[key]
                        except Exception as e:
                            print("error: ", e)
                            pass
                        finally:
                            del self.adset_insights[key]
                else:
                    self.adset_insights["purchase"] = self.adset_insights.pop(self.optimization_goal)
            return self.adset_insights
        
    def retrieve_all(self, date_preset=None):
        self.get_adset_features()
        self.get_adset_insights(date_preset=date_preset)
        self.adset_features[ Field.adset_id ] = self.adset_features.pop('id')
        self.info = { **self.adset_insights, **self.adset_features }
        return self.info


# In[6]:


def data_collect( campaign_id, total_clicks ):
    camp = Campaigns( campaign_id )
    life_time_campaign_status = camp.retrieve_all( date_preset=DatePreset.lifetime )
    period_left = (camp.ai_stop_date-datetime.datetime.now().date()).days + 1
    print('total_clicks', total_clicks)
    target_left = int(total_clicks)
    target_pair = {
        "purchase": 0,
        "cost_per_purchase": 0
    }
    if camp.campaign_insights["purchase"] != 0:
        target_pair["cost_per_purchase"] = camp.campaign_insights[Field.spend] / camp.campaign_insights["purchase"]
        target_pair["purchase"] = camp.campaign_insights["purchase"]
        target_left -= int(life_time_campaign_status[ "purchase" ])     
    target_pair[Field.target] = target_pair.pop("purchase")
    target_pair[Field.cost_per_target] = target_pair.pop("cost_per_purchase")
                           
    campaign_status = {
        'charge_type': camp.charge_type,
        'destination': total_clicks,
        'target_left': target_left,
        'daily_charge': target_left / period_left,
    }
    campaign_dict = {
        **camp.campaign_features,
        **target_pair,
        **campaign_status,
        Field.spend:camp.campaign_insights[Field.spend],
        Field.impressions:camp.campaign_insights[Field.impressions]
    }
    df_camp = pd.DataFrame(campaign_dict, index=[0])
    df_camp[df_camp.columns] = df_camp[df_camp.columns].apply(pd.to_numeric, errors='ignore')
    campaign_conv_metrics = {
        **camp.campaign_insights,
        Field.campaign_id:campaign_id
    }
    print(campaign_conv_metrics)
    adset_list = camp.get_adsets_active()
    for adset_id in adset_list:
        adset = AdSets(adset_id)
        adset_dict = adset.retrieve_all(date_preset=DatePreset.today)
        adset_dict['request_time'] = datetime.datetime.now()
        adset_dict['campaign_id'] = adset.campaign_id
        df_adset = pd.DataFrame(adset_dict, index=[0])
        insertion("adset_conversion_metrics", df_adset)
        del adset
    del camp
    update_campaign_target(df_camp)
    check_conv_metrics(campaign_id, campaign_conv_metrics)
    return


# In[7]:


def main():
    start_time = datetime.datetime.now()
    
    df_camp = mysql_saver.get_running_custom_conversion_campaign()
#     print(df_camp)

    for campaign_id in df_camp.campaign_id.unique().tolist():
        print('[main] campaign_id', campaign_id)
        destination = df_camp['destination'][df_camp.campaign_id==campaign_id].iloc[0]
        data_collect( campaign_id, destination )#存資料
        
    print(datetime.datetime.now()-start_time)
    import gc
    gc.collect()


# In[8]:


if __name__ == "__main__":
    main()


# In[ ]:





# In[9]:


#!jupyter nbconvert --to script facebook_custom_conversion_datacollector.ipynb


# In[ ]:




