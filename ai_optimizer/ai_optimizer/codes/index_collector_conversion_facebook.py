#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
from facebook_business.api import FacebookAdsApi
import facebook_business.adobjects.adaccount as adaccount
import facebook_business.adobjects.adset as adset
import facebook_business.adobjects.ad as ad
import facebook_business.adobjects.campaign as campaign
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adactivity import AdActivity
from facebook_business.adobjects.insightsresult import InsightsResult
from facebook_business.adobjects.adsinsights import AdsInsights
my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBALrHTgMWgRujnWcZA3ZB823phs6ynDDtQxnzIZASyRQZCHfr5soXBZA7NM9Dc4j9O8FtnlIzxiPCsYt4tmPQ6ZAT3yJLPuYQqjnWZBWX5dsOVzNhEqsHYj1jVJ3RAVVueW7RSxRDbNXKvK3W23dcAjNMjxIjQGIOgZDZD'

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
import facebook_custom_conversion_datacollector as custom_conv_collector
import json
import datetime
import pandas as pd
from bid_operator import *
import math

# In[2]:


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


# In[3]:


CAMPAIGN_FIELD = {
    'spend_cap': campaign.Campaign.Field.spend_cap,
    'objective': campaign.Campaign.Field.objective,
    'start_time': campaign.Campaign.Field.start_time,
    'stop_time': campaign.Campaign.Field.stop_time,
}
ADSET_FIELD = {
    'bid_amount': adset.AdSet.Field.bid_amount,
    'daily_budget': adset.AdSet.Field.daily_budget
}


# In[4]:

GENERAL_FIELD = {
    'impressions': AdsInsights.Field.impressions,
    'reach': AdsInsights.Field.reach,
    'spend': AdsInsights.Field.spend,
    'cpc': AdsInsights.Field.cpc,
    'clicks': AdsInsights.Field.clicks,
}
TARGET_FIELD = {
    'actions': AdsInsights.Field.actions,
    'cost_per_actions': AdsInsights.Field.cost_per_action_type,
}
GENERAL_INSIGHTS = {
    'impressions': AdsInsights.Field.impressions,
    'spend': AdsInsights.Field.spend,
    'ctr': AdsInsights.Field.ctr
}
TARGET_INSIGHTS = {
    'actions': AdsInsights.Field.actions,
#     'cost_per_actions': AdsInsights.Field.cost_per_action_type,
}
CONVERSION_METRICS = {
    'offsite_conversion.fb_pixel_add_to_cart': 'add_to_cart',
    'offsite_conversion.fb_pixel_initiate_checkout': 'initiate_checkout',
    'offsite_conversion.fb_pixel_purchase': 'purchase',
    'offsite_conversion.fb_pixel_view_content': 'view_content',
    'landing_page_view': 'landing_page_view',
    'link_click': 'link_click'
}
CONVERSION_CAMPAIGN_LIST = [
    'CONVERSIONS', 'ADD_TO_CART']

# In[5]:


class Field:
    ai_spend_cap = 'ai_spend_cap'
    ai_start_date = 'ai_start_date'
    ai_stop_date = 'ai_stop_date'
    target_type = 'target_type'
    target = 'target'
    cost_per_target = 'cost_per_target'
    charge_type = 'charge_type'
    start_time = 'start_time'
    stop_time = 'stop_time'
    ai_start_date = 'ai_start_date'
    ai_stop_date = 'ai_stop_date'
    period = 'period'
    daily_budget = 'daily_budget'
    account_id = 'account_id'
    actions = 'actions'
    adset_id = 'adset_id'
    campaign_id = 'campaign_id'
    clicks = 'clicks'
    targeting = 'targeting'
    age_max = 'age_max'
    age_min = 'age_min'
    flexible_spec = 'flexible_spec'
    geo_locations = 'geo_locations'
    conversion_values = 'conversion_values'
    conversions = 'conversions'
    purchase = 'purchase'
    cost_per_purchase = 'cost_per_purchase'
    cost_per_10_sec_video_view = 'cost_per_10_sec_video_view'
    cost_per_15_sec_video_view = 'cost_per_15_sec_video_view'
    cost_per_2_sec_continuous_video_view = 'cost_per_2_sec_continuous_video_view'
    cost_per_action_type = 'cost_per_action_type'
    cost_per_ad_click = 'cost_per_ad_click'
    cost_per_conversion = 'cost_per_conversion'
    cost_per_dda_countby_convs = 'cost_per_dda_countby_convs'
    cost_per_estimated_ad_recallers = 'cost_per_estimated_ad_recallers'
    cost_per_inline_link_click = 'cost_per_inline_link_click'
    cost_per_inline_post_engagement = 'cost_per_inline_post_engagement'
    cost_per_one_thousand_ad_impression = 'cost_per_one_thousand_ad_impression'
    cost_per_outbound_click = 'cost_per_outbound_click'
    cost_per_thruplay = 'cost_per_thruplay'
    cost_per_unique_action_type = 'cost_per_unique_action_type'
    cost_per_unique_click = 'cost_per_unique_click'
    cost_per_unique_conversion = 'cost_per_unique_conversion'
    cost_per_unique_inline_link_click = 'cost_per_unique_inline_link_click'
    cost_per_unique_outbound_click = 'cost_per_unique_outbound_click'
    cpc = 'cpc'
    cpm = 'cpm'
    cpp = 'cpp'
    created_time = 'created_time'
    ctr = 'ctr'
    frequency = 'frequency'
    frequency_value = 'frequency_value'
    impressions = 'impressions'
    inline_link_click_ctr = 'inline_link_click_ctr'
    inline_link_clicks = 'inline_link_clicks'
    inline_post_engagement = 'inline_post_engagement'
    objective = 'objective'
    outbound_clicks = 'outbound_clicks'
    outbound_clicks_ctr = 'outbound_clicks_ctr'
    product_id = 'product_id'
    purchase_roas = 'purchase_roas'
    reach = 'reach'
    relevance_score = 'relevance_score'
    spend = 'spend'
    spend_cap = 'spend_cap'
    lifetime_budget = 'lifetime_budget'
    unique_actions = 'unique_actions'
    unique_clicks = 'unique_clicks'
    unique_conversions = 'unique_conversions'
    unique_ctr = 'unique_ctr'
    unique_inline_link_click_ctr = 'unique_inline_link_click_ctr'
    unique_inline_link_clicks = 'unique_inline_link_clicks'
    unique_link_clicks_ctr = 'unique_link_clicks_ctr'
    unique_outbound_clicks = 'unique_outbound_clicks'
    unique_outbound_clicks_ctr = 'unique_outbound_clicks_ctr'
    unique_video_continuous_2_sec_watched_actions = 'unique_video_continuous_2_sec_watched_actions'
    unique_video_view_10_sec = 'unique_video_view_10_sec'
    unique_video_view_15_sec = 'unique_video_view_15_sec'
    video_10_sec_watched_actions = 'video_10_sec_watched_actions'
    video_15_sec_watched_actions = 'video_15_sec_watched_actions'
    video_30_sec_watched_actions = 'video_30_sec_watched_actions'
    video_avg_percent_watched_actions = 'video_avg_percent_watched_actions'
    video_avg_time_watched_actions = 'video_avg_time_watched_actions'
    video_continuous_2_sec_watched_actions = 'video_continuous_2_sec_watched_actions'
    video_p100_watched_actions = 'video_p100_watched_actions'
    video_p25_watched_actions = 'video_p25_watched_actions'
    video_p50_watched_actions = 'video_p50_watched_actions'
    video_p75_watched_actions = 'video_p75_watched_actions'
    video_p95_watched_actions = 'video_p95_watched_actions'
    video_play_actions = 'video_play_actions'
    video_play_retention_0_to_15s_actions = 'video_play_retention_0_to_15s_actions'
    video_play_retention_20_to_60s_actions = 'video_play_retention_20_to_60s_actions'
    video_play_retention_graph_actions = 'video_play_retention_graph_actions'
    video_thruplay_watched_actions = 'video_thruplay_watched_actions'
    video_time_watched_actions = 'video_time_watched_actions'
    website_ctr = 'website_ctr'
    website_purchase_roas = 'website_purchase_roas'
    
class DatePreset:
    today = 'today'
    yesterday = 'yesterday'
    this_month = 'this_month'
    last_month = 'last_month'
    this_quarter = 'this_quarter'
    lifetime = 'lifetime'
    last_3d = 'last_3d'
    last_7d = 'last_7d'
    last_14d = 'last_14d'
    last_28d = 'last_28d'
    last_30d = 'last_30d'
    last_90d = 'last_90d'
    last_week_mon_sun = 'last_week_mon_sun'
    last_week_sun_sat = 'last_week_sun_sat'
    last_quarter = 'last_quarter'
    last_year = 'last_year'
    this_week_mon_today = 'this_week_mon_today'
    this_week_sun_today = 'this_week_sun_today'
    this_year = 'this_year'
    


# In[6]:


class Campaigns(object):
    def __init__( self, campaign_id ):
        self.campaign_id = campaign_id
        self.campaign_insights = dict()
        columns=['impressions', 'spend', 'landing_page_view', 'cost_per_landing_page_view', 'link_click', 'cost_per_link_click', 'add_to_cart', 'cost_per_add_to_cart', 'initiate_checkout', 'cost_per_initiate_checkout', 'purchase', 'cost_per_purchase', 'view_content', 'cost_per_view_content']
        for col in columns:
            self.campaign_insights.setdefault(col, 0)
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
        adcamps = ad_campaign.remote_read( fields=list(CAMPAIGN_FIELD.values()) )
        for field in list(adcamps.keys()):
            self.campaign_features.update( {field:adcamps.get(field)} )
        return self.campaign_features
        
    def get_campaign_insights( self, date_preset=None ):
        campaigns = campaign.Campaign( self.campaign_id )
        self.optimization_goal = 'offsite_conversion.custom.'+self.custom_conversion_id if self.custom_conversion_id else None
        self.temp_campaign_insights = dict()
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
                fields=list( GENERAL_FIELD.values() )+list( TARGET_FIELD.values() )
            )
        except:
            insights = campaigns.get_insights(
                fields=list( GENERAL_FIELD.values() )+list( TARGET_FIELD.values() )
            )
        if len(insights) > 0:
            current_campaign = insights[0]
            if current_campaign.get(Field.impressions) and current_campaign.get(Field.spend):
                spend = current_campaign.get(Field.spend)
                impressions = current_campaign.get(Field.impressions)
                self.campaign_insights.update( { Field.spend: int(float(spend)) } )
                self.campaign_insights.update( { Field.impressions: int(impressions) } )
                try:
                    for act in current_campaign.get( Field.actions ):
                        if act["action_type"] in CONVERSION_METRICS:
                            self.campaign_insights.update({
                                CONVERSION_METRICS[ act["action_type"] ]: int(act["value"]) 
                            })
                            self.campaign_insights.update({
                                'cost_per_' + CONVERSION_METRICS[ act["action_type"] ] : float(spend) / float(act["value"])
                            })
                except Exception as e:
                    print('[conversion_index_collector.Campaigns.get_campaign_insights]', e)
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


# In[7]:


class AdSets(object):
    def __init__( self, adset_id ):
        self.adset_id = adset_id
        self.adset_features = dict()
        self.adset_insights = dict()
        columns=['impressions', 'spend', 'landing_page_view', 'cost_per_landing_page_view', 'link_click', 'cost_per_link_click', 'add_to_cart', 'cost_per_add_to_cart', 'initiate_checkout', 'cost_per_initiate_checkout', 'purchase', 'cost_per_purchase', 'view_content', 'cost_per_view_content']
        for col in columns:
            self.adset_insights.setdefault(col, 0)
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
        adsets = adsets.remote_read( fields=list( ADSET_FIELD.values() ) )
        for field in list(adsets.keys()):
            self.adset_features.update( { field:adsets.get(field) } )
        return self.adset_features
    
    def get_adset_insights( self, date_preset=None ):
        adsets = adset.AdSet( self.adset_id )
        self.optimization_goal = 'offsite_conversion.custom.'+self.custom_conversion_id if self.custom_conversion_id else None
        self.temp_adset_insights = dict()
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
                self.adset_insights.update( { Field.spend: int(float(spend)) } )
                self.adset_insights.update( { Field.impressions: int(impressions) } )
            try:
                for act in insights[0].get( Field.actions ):
                    if act["action_type"] in CONVERSION_METRICS:
                        self.adset_insights.update( {
                            CONVERSION_METRICS[ act["action_type"] ]: int(act["value"])
                        } )
                        self.adset_insights.update( {
                            'cost_per_' + CONVERSION_METRICS[ act["action_type"] ] : float(spend) / float(act["value"])
                        } )
            except Exception as e:
                print('[conversion_index_collector_facebook.AdSets.get_adset_insights]', e)
            finally:
                return self.adset_insights
        
    def retrieve_all(self, date_preset=None):
        self.get_adset_features()
        self.get_adset_insights(date_preset=date_preset)
        self.adset_features[ Field.adset_id ] = self.adset_features.pop('id')
        self.info = { **self.adset_insights, **self.adset_features }
        return self.info


# In[8]:


def data_collect( campaign_id, total_clicks ):
    camp = Campaigns( campaign_id )
    life_time_campaign_status = camp.retrieve_all( date_preset=DatePreset.lifetime )
    period_left = (camp.ai_stop_date-datetime.datetime.now().date()).days + 1
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


# In[9]:


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


# In[10]:


def insertion(table, df):
    engine = create_engine( 'mysql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE) )
    with engine.connect() as conn, conn.begin():
        df.to_sql(table, conn, if_exists='append',index=False)
        engine.dispose()


# In[11]:


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

# In[12]:


def check_conv_metrics(campaign_id, campaign_conv_metrics):
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
        
def check_optimal_weight(campaign_id, df):
    mydb = connectDB(DATABASE)
    df_check = pd.read_sql( "SELECT * FROM conversion_optimal_weight WHERE campaign_id=%s" % (campaign_id), con=mydb )
    
    if df_check.empty:
        engine = create_engine( 'mysql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE) )
        with engine.connect() as conn, conn.begin():
            df.to_sql("conversion_optimal_weight", conn, if_exists='append',index=False)
            engine.dispose()
    else:
        df.drop(['campaign_id'], axis=1)
        mycursor = mydb.cursor()
        for column in df.columns:
            try:
                sql = ("UPDATE conversion_optimal_weight SET {} = '{}' WHERE campaign_id={}".format(column, df[column].iloc[0], campaign_id))
                mycursor.execute(sql)
                mydb.commit()
            except Exception as e:
                print('[gdn_db.update_table]: ', e)
                pass
        mycursor.close()
    mydb.close()
    return 


# In[13]:
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

def get_campaign_target():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now().date()
    df = pd.read_sql( "SELECT * FROM campaign_target WHERE ai_status='active'" , con=mydb )
    campaignid_list = df['campaign_id'].unique()
    df_camp = pd.DataFrame(columns=df.columns)
    for campaign_id in campaignid_list:
        stop_time = df['ai_stop_date'][df.campaign_id==campaign_id].iloc[0]
        start_time = df['ai_start_date'][df.campaign_id==campaign_id].iloc[0]
        if stop_time >= request_time and start_time <= request_time:
            df_temp = df[df.campaign_id==campaign_id]
            df_camp = pd.concat([df_camp, df_temp])
    mydb.close()
    return df_camp

def get_campaign_is_running():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now().date()
    df = pd.read_sql( "SELECT * FROM campaign_target WHERE ai_status='active'" , con=mydb )
    campaignid_list = df['campaign_id'].unique()
    df_camp = pd.DataFrame(columns=df.columns)
    df_is_running = df.drop( df[ df['ai_stop_date'] <= request_time ].index )
    mydb.close()
    return df_is_running

def get_conversion_campaign_is_running():
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now().date()
    df = pd.read_sql( "SELECT * FROM campaign_target WHERE ai_status='active'" , con=mydb )
    campaignid_list = df['campaign_id'].unique()
    df_camp = pd.DataFrame(columns=df.columns)
    df_is_running = df.drop( df[ df['ai_stop_date'] <= request_time ].index )
    df_conversion_is_running = df_is_running.drop( df_is_running[ df_is_running['charge_type'] != 'CONVERSIONS' ].index )
    mydb.close()
    return df_conversion_is_running

def get_running_conversion_campaign(campaign_id=None):
    mydb = connectDB(DATABASE)
    request_time = datetime.datetime.now().date()
    if campaign_id is None:
        df = pd.read_sql( "SELECT * FROM campaign_target WHERE ai_status='active'", con=mydb )
        df = df[ (df['target_type'].isin(CONVERSION_CAMPAIGN_LIST)) & (df.ai_stop_date >= request_time) ]
        mydb.close()
        return df
    else:
        df = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id='{}' AND ai_status='active'".format(campaign_id), con=mydb )
        
        mydb.close()
        return df

# In[14]:


def main(campaign_id=None, destination=None):
    start_time = datetime.datetime.now()
    if campaign_id:
        data_collect( campaign_id, destination )#存資料
    else:
        df_camp = get_running_conversion_campaign()
        for campaign_id in df_camp['campaign_id'][df_camp.custom_conversion_id.isna()].unique().tolist():
            destination = df_camp['destination'][df_camp.campaign_id==campaign_id].iloc[0]
            data_collect( campaign_id, destination )#存資料
        for campaign_id in df_camp['campaign_id'][df_camp.custom_conversion_id.notna()].unique().tolist():
            print('[not standard campaign]: id', campaign_id)
            destination = df_camp['destination'][df_camp.campaign_id==campaign_id].iloc[0]
            custom_conv_collector.data_collect( campaign_id, destination )#存資料
    print(datetime.datetime.now()-start_time)
    import gc
    gc.collect()


# In[2]:


if __name__ == "__main__":
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    main()


# In[ ]:


#!jupyter nbconvert --to script index_collector_conversion_facebook.ipynb


# In[ ]:




