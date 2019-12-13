#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import facebook_business.adobjects.adset as adset
from facebook_business.adobjects.ad import Ad
import facebook_business.adobjects.campaign as campaign
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adactivity import AdActivity
from facebook_business.adobjects.insightsresult import InsightsResult
from facebook_business.adobjects.adsinsights import AdsInsights
# my_app_id = '958842090856883'
# my_app_secret = 'a952f55afca38572cea2994d440d674b'
# my_access_token = 'EAANoD9I4obMBACygIE9jqmlaWeOW6tBma0oS6JbRpLgAvOYXpVi2XcXuasuwbBgqmaZBj5cP8MHE5WY2l9tAoi549eGZCP61mKr9BA8rZA6kxEW4ovX3KlbbrRGgt4RZC8MAi1UG0l0ZBUd0UBAhIPhzkZBi46ncuyCwkYPB7a6voVBZBTbEZAwH3azZA3Ph6g7aCOfxZCdDOp4AZDZD'

# FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

import json
import datetime
import pandas as pd
import math
import random

import facebook_currency_handler as currency_handler
import adgeek_permission as permission
import database_controller
from bid_operator import *


# In[2]:



CAMPAIGN_OBJECTIVE_FIELD = {
    'LINK_CLICKS': 'link_click',
    'POST_ENGAGEMENT': 'post_engagement', 
    'LANDING_PAGE_VIEW': 'landing_page_view',
    'VIDEO_VIEWS': 'video_view', 
    'CONVERSIONS':'offsite_conversion.fb_pixel_purchase',
    'PURCHASE':'offsite_conversion.fb_pixel_purchase',
    'ADD_TO_CART':'offsite_conversion.fb_pixel_add_to_cart',
    'THRUPLAY': 'video_view',
    'APP_INSTALLS': 'app_installs',
    'BRAND_AWARENESS': 'brand_awareness',
    'EVENT_RESPONSES': 'event_responses',
    'LEAD_GENERATION': 'leadgen.other',
    'LOCAL_AWARENESS': 'local_awareness',
    'OFFER_CLAIMS': 'offer_claims',
    'PAGE_LIKES': 'like',
    'PRODUCT_CATALOG_SALES': 'product_catalog_sales',
    'REACH': 'reach',
    'ALL_CLICKS': 'clicks',
    'IMPRESSIONS': 'impressions',
    'COMPLETE_REGISTRATION': 'offsite_conversion.fb_pixel_complete_registration',
    'VIEW_CONTENT': 'offsite_conversion.fb_pixel_view_content',
    'ADD_PAYMENT_INFO': 'offsite_conversion.fb_pixel_add_payment_info',
    'ADD_TO_WISHLIST': 'offsite_conversion.fb_pixel_add_to_wishlist',
    'LEAD_WEBSITE': 'offsite_conversion.fb_pixel_lead',
    'PURCHASES': 'offsite_conversion.fb_pixel_purchase',
    'INITIATE_CHECKOUT': 'offsite_conversion.fb_pixel_initiate_checkout',
    'SEARCH': 'offsite_conversion.fb_pixel_search',
    'MESSAGES': 'onsite_conversion.messaging_reply',
    "CUSTOM": "offsite_conversion.custom."
}
CAMPAIGN_FIELD = {
    'spend_cap': campaign.Campaign.Field.spend_cap,
    'objective': campaign.Campaign.Field.objective,
    'start_time': campaign.Campaign.Field.start_time,
    'stop_time': campaign.Campaign.Field.stop_time,
}
ADSET_FIELD = {
    'optimization_goal':adset.AdSet.Field.optimization_goal,
    'bid_amount':adset.AdSet.Field.bid_amount,
    'daily_budget':adset.AdSet.Field.daily_budget,
    'targeting':adset.AdSet.Field.targeting,
    'status':adset.AdSet.Field.status
}
CAMPAIGN_INSIGHTS_FIELD = {
    'campaign_id': AdsInsights.Field.campaign_id,
}
ADSET_INSIGHTS_FIELD = {
    'adset_id': AdsInsights.Field.adset_id,
}
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
    'video_thruplay_watched_actions': AdsInsights.Field.video_thruplay_watched_actions ,
    'cost_per_thruplay': AdsInsights.Field.cost_per_thruplay
}
GENERAL_INSIGHTS = {
    'impressions': AdsInsights.Field.impressions,
    'spend': AdsInsights.Field.spend,
    'ctr': AdsInsights.Field.ctr
}
TARGET_INSIGHTS = {
    'actions': AdsInsights.Field.actions,
}
FUNNEL_METRICS = {
    "CUSTOM": {
        "offsite_conversion.fb_pixel_purchase": "action",
        "offsite_conversion.fb_pixel_add_to_cart": "desire",
        "offsite_conversion.fb_pixel_view_content": "interest",
        "landing_page_view": "awareness",
    },
    "CONVERSIONS": {
        "offsite_conversion.fb_pixel_purchase": "action",
        "offsite_conversion.fb_pixel_add_to_cart": "desire",
        "offsite_conversion.fb_pixel_view_content": "interest",
        "landing_page_view": "awareness",
    },
    "PURCHASE": {
        "offsite_conversion.fb_pixel_purchase": "action",
        "offsite_conversion.fb_pixel_add_to_cart": "desire",
        "offsite_conversion.fb_pixel_view_content": "interest",
        "landing_page_view": "awareness",
    },
    "LEAD_WEBSITE": {
        "offsite_conversion.fb_pixel_lead":"action",
        "offsite_conversion.fb_pixel_view_content":"desire",
        "landing_page_view":"interest",
        "link_click": "awareness",
    },
    "LINK_CLICKS": {
        "link_click": "action",
    },
    "POST_ENGAGEMENT": {
        "post_engagement": "action",
    },
    "VIDEO_VIEWS": {
        "video_view": "action",
    },
    "THRUPLAY": {
        "video_view": "action",
    },
    "LEAD_GENERATION": {
        "leadgen.other": "action",
    },
    "PAGE_LIKES": {
        "like": "action",
    },
    "COMPLETE_REGISTRATION": {
        "offsite_conversion.fb_pixel_complete_registration": "action",
    },
    "VIEW_CONTENT": {
        "offsite_conversion.fb_pixel_view_content": "action",
    },
    "ADD_PAYMENT_INFO": {
        "offsite_conversion.fb_pixel_add_payment_info": "action",
    },
    "ADD_TO_WISHLIST": {
        "offsite_conversion.fb_pixel_add_to_wishlist": "action",
    },
    "INITIATE_CHECKOUT": {
        "offsite_conversion.fb_pixel_initiate_checkout": "action",
    },
    "SEARCH": {
        "offsite_conversion.fb_pixel_search": "action",
    },
}
FUNNEL_LIST = ['action', 'desire', 'interest', 'awareness', 'spend']
BRANDING_CAMPAIGN_LIST = [
    'THRUPLAY', 'LINK_CLICKS', 'ALL_CLICKS', 'VIDEO_VIEWS', 'REACH', 'POST_ENGAGEMENT', 'PAGE_LIKES', 'LANDING_PAGE_VIEW']
PERFORMANCE_CAMPAIGN_LIST = [
    'CUSTOM', 'MESSAGES', 'SEARCH', 'INITIATE_CHECKOUT', 'LEAD_WEBSITE', 'PURCHASE', 'ADD_TO_WISHLIST', 'VIEW_CONTENT', 'ADD_PAYMENT_INFO', 'COMPLETE_REGISTRATION', 'CONVERSIONS', 'LEAD_GENERATION', 'ADD_TO_CART']


# In[3]:


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

    
class Status:
    active = 'ACTIVE'
    paused = 'PAUSED'
    
    
class Field:
    ai_spend_cap = 'ai_spend_cap'
    ai_start_date = 'ai_start_date'
    ai_stop_date = 'ai_stop_date'
    target_type = 'target_type'
    target = 'target'
    cost_per_target = 'cost_per_target'
    charge_type = 'charge_type'
    destination_type = 'destination_type'
    start_time = 'start_time'
    stop_time = 'stop_time'
    period = 'period'
    daily_budget = 'daily_budget'
    bid_amount = 'bid_amount'
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
    status = 'status'
    conversion_values = 'conversion_values'
    conversions = 'conversions'
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
    video_thruplay_watched_actions = 'video_thruplay_watched_actions'
    cost_per_thruplay = 'cost_per_thruplay'
    website_ctr = 'website_ctr'
    website_purchase_roas = 'website_purchase_roas'
    


# In[4]:


class Accounts(object):
    def __init__( self, account_id ):
        self.account_id = account_id
        self.account_insights = dict()
        
    def get_account_insights( self, date_preset=DatePreset.yesterday ):
        accounts = AdAccount( self.account_id )
        params = {
            'date_preset': date_preset,
        }
        insights = accounts.get_insights(
            params=params,
            fields=list( GENERAL_FIELD.values() )+list( TARGET_FIELD.values() )
        )
        current_account = insights[0]
        return current_account


# In[5]:


class Campaigns(object):
    def __init__( self, campaign_id, charge_type=None, database_fb=None ):
        self.campaign_id = campaign_id
        self.charge_type = charge_type
        self.campaign_insights = dict.fromkeys(FUNNEL_LIST, 0)
        self.campaign_features = dict.fromkeys(CAMPAIGN_FIELD, 0)
        self.currency = currency_handler.get_currency_by_campaign(self.campaign_id)
        if database_fb is None:
            database_fb = database_controller.FB( database_controller.Database() )
        self.brief_dict = database_fb.get_brief( self.campaign_id )
        self.ai_spend_cap = self.brief_dict[Field.ai_spend_cap]
        self.ai_start_date = self.brief_dict[Field.ai_start_date]
        self.ai_stop_date = self.brief_dict[Field.ai_stop_date]
        self.charge_type = self.brief_dict[Field.destination_type]
        self.destination_type = self.brief_dict[Field.destination_type]
        self.custom_conversion_id = self.brief_dict.get("custom_conversion_id")
        
    # Getters

    def get_campaign_features( self ):
        ad_campaign = campaign.Campaign( self.campaign_id )
        adcamps = ad_campaign.api_get( fields = list(CAMPAIGN_FIELD.values()) )
        for campaign_field in list(adcamps.keys()):
            self.campaign_features.update( {campaign_field:adcamps.get(campaign_field)} )
        return self.campaign_features
    
    def get_campaign_insights( self, date_preset=None ):
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
        camp = campaign.Campaign( self.campaign_id )
        try:
            insights = camp.get_insights(
                params=params,
                fields=list( GENERAL_FIELD.values() )+list( TARGET_FIELD.values() )
            )
        except:
            insights = camp.get_insights(
                fields=list( GENERAL_FIELD.values() )+list( TARGET_FIELD.values() )
            )
        if len(insights) > 0:
            current_campaign = insights[0]
            if current_campaign.get(Field.impressions):
                spend = float( current_campaign.get( Field.spend ) )
                
            for campaign_field in list( GENERAL_FIELD.keys() ):
                self.campaign_insights.update( {campaign_field: current_campaign.get(campaign_field)} )
            # Deal with those metrics not in 'actions' metric
            if self.charge_type == 'ALL_CLICKS':
                '''assign to field target and cost_per_target'''
                self.campaign_insights[ "action" ] = int(self.campaign_insights.pop( Field.clicks ))
                
            elif self.charge_type == 'REACH':
                self.campaign_insights[ "action" ] = int(self.campaign_insights[ Field.reach ])
                
            elif self.charge_type == 'IMPRESSIONS':
                self.campaign_insights[ "action" ] = int(self.campaign_insights[ Field.impressions ])
                
            elif self.charge_type in ['THRUPLAY', 'VIDEO_VIEWS']:
                actions_list = current_campaign.get( TARGET_FIELD[Field.video_thruplay_watched_actions], [] )
                for act in actions_list:
                    if act["action_type"] == CAMPAIGN_OBJECTIVE_FIELD[ self.charge_type ]:
                        target = int( act.get("value") ) if act.get("value") else 0
                        self.campaign_insights.update( {"action": target} )
                        
            elif self.charge_type in PERFORMANCE_CAMPAIGN_LIST+BRANDING_CAMPAIGN_LIST:
                actions_list = current_campaign.get( Field.actions )
                actions_dict = FUNNEL_METRICS.get( self.charge_type )
                if actions_list:
                    if self.custom_conversion_id:
                        custom_conversion_key = 'offsite_conversion.custom.' + str(self.custom_conversion_id)
                        insights = dict( { custom_conversion_key: 0 } )
                        action_type_list = [act["action_type"] for act in actions_list]
                        action_value_list = [int(act["value"]) for act in actions_list]
                        insights.update( dict( zip( action_type_list, action_value_list ) ) )
                        for key, val in insights.copy().items():
                            if val < insights[custom_conversion_key]:
                                insights.pop(key)
                        value_list = list(sorted(set(insights.values()), reverse=False))[:4]
                        values = set([insights[custom_conversion_key]])
                        for key, val in insights.copy().items():
                            if key != custom_conversion_key:
                                if val in values: 
                                    del insights[key]
                                else:
                                    values.add(val)
                        for key, val in insights.copy().items():
                            if val not in value_list:
                                insights.pop(key)
                        funnel_dict = dict( zip( insights.keys(), FUNNEL_LIST ) )
                        actual_metrics_list = list(insights.keys())
                        insights = dict((funnel_dict[key], value) for (key, value) in insights.items())
                        insights.update({"actual_metrics": str(actual_metrics_list)})
                        self.campaign_insights.update( insights )

                    else:
                        action_type_list = [actions_dict.get(act["action_type"]) for act in actions_list if act["action_type"] in actions_dict]
                        print("[action_type_list]: ", action_type_list)
                        action_value_list = [int(act["value"]) for act in actions_list if act["action_type"] in actions_dict]
                        self.campaign_insights.update(
                            dict( zip( action_type_list, action_value_list ) )
                        )
            self.campaign_insights.pop( Field.clicks, None )
            self.campaign_insights.pop( Field.cpc, None )
            self.currency_handle()
        return self.campaign_insights

    def get_adsets( self ):
        adset_list=list()
        camp = campaign.Campaign( self.campaign_id )
        adsets = camp.get_ad_sets( fields = [adset.AdSet.Field.id ,  adset.AdSet.Field.status])
        adset_list = [ adset.get("id") for adset in adsets ]
        return adset_list
    
    def get_adsets_active(self):
        adset_active_list = list()
        camp = campaign.Campaign( self.campaign_id )
        adsets = camp.get_ad_sets( fields = [adset.AdSet.Field.id ,  adset.AdSet.Field.status])
        adset_active_list = [
            adset.get("id") for adset in adsets if adset.get("status") == 'ACTIVE'
        ]
        print('[get_adsets_active] adset_active_list:', adset_active_list )
        return adset_active_list

    def get_account_id( self ):
        camp = campaign.Campaign( self.campaign_id )
        account = camp.get_ad_sets(fields=[campaign.Campaign.Field.account_id])
        current_account = account[0]
        return current_account.get( Field.account_id )
        
    # Operator
    
    def currency_handle( self ):
        if self.currency in currency_handler.OFFSET_A_HUNDRED:
            self.campaign_insights['spend'] = float(self.campaign_insights['spend']) * 100
    
    def generate_info( self, date_preset=DatePreset.lifetime ):
        self.get_campaign_features()
        self.get_campaign_insights( date_preset )
        self.campaign_features[ Field.campaign_id ] = self.campaign_features.pop('id')
        self.campaign_features[ Field.target_type ] = self.campaign_features.pop('objective')
        start_time_str = str(self.campaign_features[Field.start_time])[:-5]
        self.campaign_features[ Field.start_time ] = datetime.datetime.strptime( start_time_str,'%Y-%m-%dT%H:%M:%S' )
        self.campaign_features[ Field.period ] = ( self.ai_stop_date - self.ai_start_date ).days + 1
        self.campaign_features[ Field.start_time ] = self.campaign_features[Field.start_time].strftime( '%Y-%m-%d %H:%M:%S' )
        self.campaign_features[ Field.daily_budget ] = int( self.ai_spend_cap )/self.campaign_features[Field.period]
        self.campaign_info = { **self.campaign_insights, **self.campaign_features }
        return self.campaign_info


# In[6]:


class AdSets(object):
    def __init__( self, adset_id, charge_type=None, database_fb=None ):
        self.adset_id = adset_id
        self.charge_type = charge_type
        self.adset_features = dict.fromkeys(ADSET_FIELD, 0)
        self.adset_features.pop('targeting')
        self.adset_insights = dict.fromkeys(FUNNEL_LIST, 0)
        self.adset_info = dict()

        self.campaign_id = self.get_campaign_id()
        self.currency = currency_handler.get_currency_by_campaign(self.campaign_id)
        if database_fb is None:
            database_fb = database_controller.FB( database_controller.Database() )
        self.brief_dict = database_fb.get_brief( self.campaign_id )
        self.ai_spend_cap = self.brief_dict.get(Field.ai_spend_cap)
        self.ai_start_date = self.brief_dict.get(Field.ai_start_date)
        self.ai_stop_date = self.brief_dict.get(Field.ai_stop_date)
        self.charge_type = self.brief_dict.get(Field.destination_type)
        self.destination_type = self.brief_dict.get(Field.destination_type)
        self.custom_conversion_id = self.brief_dict.get("custom_conversion_id")
        
    # Getters
    
    def get_ads( self ):
        ad_list=list()
        adsets = adset.AdSet( self.adset_id )
        try:
            ads = adsets.get_ads( fields = [ Ad.Field.id ])
        except Exception as e:
            print('[AdSets.get_ads] adset id: ', self.adset_id)
            print('[AdSets.get_ads] error: ', e)
        for ad in ads:
            ad_list.append( ad.get("id") )
        return ad_list
    
    def get_fb_pixel_id( self ):
        adsets = adset.AdSet( self.adset_id )
        adsets = adsets.api_get( fields=['promoted_object'] )
        promoted_object = adsets.get("promoted_object")
        return promoted_object.get("pixel_id") if promoted_object else None
    
    def get_campaign_id( self ):
        adsets = adset.AdSet( self.adset_id )
        adsets = adsets.api_get( fields=['campaign_id'] )
        campaign_id = adsets.get("campaign_id")
        return campaign_id
    
    def get_adset_features( self ):
        adsets = adset.AdSet( self.adset_id )
        try:
            adsets = adsets.api_get( fields=list( ADSET_FIELD.values() ) )
        except Exception as e:
            print('[AdSets.get_adset_features] adset id: ', self.adset_id)
            print('[AdSets.get_adset_features] error: ', e)
            return self.adset_features.update( {'id':self.adset_id})
        for adset_field in list(adsets.keys()):
            if adset_field == Field.targeting:
                self.adset_features.update( { Field.age_max:adsets.get( Field.targeting ).get( Field.age_max ) } )
                self.adset_features.update( { Field.age_min:adsets.get( Field.targeting ).get( Field.age_min ) } )
                self.adset_features.update( { Field.flexible_spec: str(adsets.get( Field.targeting ).get( Field.flexible_spec ) ) } )
                self.adset_features.update( { Field.geo_locations: str(adsets.get( Field.targeting ).get( Field.geo_locations ) ) } )
            else:
                self.adset_features.update( { adset_field:adsets.get(adset_field) } )
        self.status = self.adset_features.pop( Field.status )
        self.adset_features.update( { Field.status: self.status } )
        if self.status == Status.active:
            self.status = True
        elif self.status == Status.paused:
            self.status = False

        return self.adset_features
    
    def get_adset_insights( self, date_preset=None ):
        adsets = adset.AdSet( self.adset_id )
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
                fields=list( GENERAL_FIELD.values() )+list( TARGET_FIELD.values() )
            )
        except Exception as e:
            insights = adsets.get_insights(
                fields=list( GENERAL_FIELD.values() )+list( TARGET_FIELD.values() )
            )
        if len(insights) > 0:
            current_adset = insights[0]
            if current_adset.get(Field.impressions):
                spend = float( current_adset.get( Field.spend ) )
                
            for adset_field in list( GENERAL_FIELD.keys() ):
                self.adset_insights.update( {adset_field: current_adset.get(adset_field)} )
                
            if self.charge_type == 'ALL_CLICKS':
                self.adset_insights[ "action" ] = int(self.adset_insights.pop( Field.clicks ))

            elif self.charge_type == 'REACH':
                self.adset_insights[ "action" ] = int(self.adset_insights[ Field.reach ])

            elif self.charge_type == 'IMPRESSIONS':
                self.adset_insights[ "action" ] = int(self.adset_insights[ Field.impressions ])

            elif self.charge_type in ['THRUPLAY', 'VIDEO_VIEWS']:
                actions_list = current_adset.get( TARGET_FIELD[Field.video_thruplay_watched_actions], [] )
                for act in actions_list:
                    if act["action_type"] == CAMPAIGN_OBJECTIVE_FIELD[ self.charge_type ]:
                        target = int( act.get("value") ) if act.get("value") else 0
                        self.adset_insights.update( {"action": target} )
                        
            elif self.charge_type in PERFORMANCE_CAMPAIGN_LIST+BRANDING_CAMPAIGN_LIST:
                actions_list = current_adset.get( Field.actions )
                actions_dict = FUNNEL_METRICS.get( self.charge_type )
                if actions_list:
                    if self.custom_conversion_id:
                        custom_conversion_key = 'offsite_conversion.custom.' + str(self.custom_conversion_id)
                        insights = dict( { custom_conversion_key: 0 } )
                        action_type_list = [act["action_type"] for act in actions_list]
                        action_value_list = [int(act["value"]) for act in actions_list]
                        insights.update( dict( zip( action_type_list, action_value_list ) ) )
                        for key, val in insights.copy().items():
                            if val < insights[custom_conversion_key]:
                                insights.pop(key)
                        value_list = list(sorted(set(insights.values()), reverse=False))[:4]
                        values = set([insights[custom_conversion_key]])
                        for key, val in insights.copy().items():
                            if key != custom_conversion_key:
                                if val in values: 
                                    del insights[key]
                                else:
                                    values.add(val)
                        for key, val in insights.copy().items():
                            if val not in value_list:
                                insights.pop(key)
                        funnel_dict = dict( zip( insights.keys(), FUNNEL_LIST ) )
                        actual_metrics_list = list(insights.keys())
                        insights = dict((funnel_dict[key], value) for (key, value) in insights.items())
                        insights.update({"actual_metrics": str(actual_metrics_list)})
                        self.adset_insights.update( insights )
                    else:
                        action_type_list = [actions_dict.get(act["action_type"]) for act in actions_list if act["action_type"] in actions_dict]
                        action_value_list = [int(act["value"]) for act in actions_list if act["action_type"] in actions_dict]
                        self.adset_insights.update(
                            dict( zip( action_type_list, action_value_list ) )
                        )
            self.adset_insights.pop( Field.clicks, None )
            self.adset_insights.pop( Field.cpc, None )
            self.currency_handle()
        return self.adset_insights

    # Operator
    
    def generate_info( self, date_preset=None ):
        self.get_adset_features()
        self.get_adset_insights( date_preset )
        self.adset_features[ Field.adset_id ] = self.adset_features.pop('id')
        self.adset_info = { **self.adset_insights, **self.adset_features }
        return self.adset_info
    
    def currency_handle( self ):
        if self.currency in currency_handler.OFFSET_A_HUNDRED:
            print('self.adset_insights:', self.adset_insights)
            self.adset_insights['spend'] = float(self.adset_insights.get('spend', 0)) * 100
    
    def update(self, bid_amount):
        adsets = adset.AdSet( self.adset_id )
        adsets.update({
            adset.AdSet.Field.bid_amount: bid_amount,
        })
        adsets.remote_update()


# In[7]:


def data_collect( data_base_fb, campaign_id, destination, charge_type, ai_start_date, ai_stop_date ):
    camp = Campaigns( campaign_id, database_fb=data_base_fb )
    life_time_campaign_insights = camp.generate_info( date_preset=DatePreset.lifetime )
    data_base_fb.upsert("campaign_metrics", {**camp.campaign_insights, **{'campaign_id': campaign_id}})
    life_time_campaign_insights["target"] = life_time_campaign_insights.pop("action")
    print('===============[life_time_campaign_insights]================')
    print(life_time_campaign_insights)
    period_left = (ai_stop_date-datetime.datetime.now().date()).days + 1
    charge = life_time_campaign_insights[ "target" ]
    life_time_campaign_insights.update({
        "cost_per_target": (int(life_time_campaign_insights["spend"]) / life_time_campaign_insights["target"]) if life_time_campaign_insights.get("target") else 0,
        "charge_type": charge_type,
        "destination": destination,
        "target_left": int(destination) - int(charge),
        "daily_charge": (int(destination) - int(charge)) / (period_left),
    })
#     print(life_time_campaign_insights)
    adset_list = camp.get_adsets_active()
    for adset_id in adset_list:
        adset = AdSets(adset_id, database_fb=data_base_fb)
        adset_dict = adset.generate_info(date_preset=DatePreset.today)
        adset_dict['campaign_id'] = campaign_id
        df_adset = pd.DataFrame(adset_dict, index=[0])
        data_base_fb.insert("adset_metrics", adset_dict)
        try:
            adset_dict['bid_amount'] = math.ceil(reverse_bid_amount(adset_dict['bid_amount']))
            df_adset = pd.DataFrame(adset_dict, index=[0])
            data_base_fb.insert_ignore("adset_initial_bid", { key : adset_dict[key] for key in [ Field.campaign_id, Field.adset_id, Field.bid_amount ] })
        except Exception as e:
            print('[datacollect] adset id: ', adset_id)
            print('[datacollect] error: ', e)
            pass
    df_camp = pd.DataFrame(life_time_campaign_insights, index=[0])
    df_camp[df_camp.columns] = df_camp[df_camp.columns].apply(pd.to_numeric, errors='ignore')
    data_base_fb.upsert("campaign_target", life_time_campaign_insights)
    return


# In[8]:


def main():
    start_time = datetime.datetime.now()

    db = database_controller.Database()
    data_base_fb = database_controller.FB(db)
#     campaign_running_list = data_base_fb.get_running_campaign().to_dict('records')
    campaign_running_list = data_base_fb.get_branding_campaign().to_dict('records')
    print([campaign['campaign_id'] for campaign in campaign_running_list])

    for campaign in campaign_running_list:
        account_id = campaign.get("account_id")
        campaign_id = campaign.get("campaign_id")
        destination = campaign.get("destination")
        charge_type = campaign.get("destination_type")
        ai_start_date = campaign.get("ai_start_date")
        ai_stop_date = campaign.get("ai_stop_date")
        custom_conversion_id = campaign.get("custom_conversion_id")
        permission.init_facebook_api(account_id)
        print(campaign_id, charge_type, custom_conversion_id)
        
        data_collect( data_base_fb, int(campaign_id), destination, charge_type, ai_start_date, ai_stop_date )
    data_base_fb.dispose()
    print(datetime.datetime.now()-start_time)


# In[9]:


if __name__ == "__main__":
    main()
    import gc
    gc.collect()


# In[10]:


# !jupyter nbconvert --to script facebook_datacollector.ipynb

