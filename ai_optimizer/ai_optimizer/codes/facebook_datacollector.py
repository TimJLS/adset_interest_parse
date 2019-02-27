from pathlib import Path
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adactivity import AdActivity
from facebook_business.adobjects.insightsresult import InsightsResult
from facebook_business.adobjects.adsinsights import AdsInsights
my_app_id = '958842090856883'
my_app_secret = 'a952f55afca38572cea2994d440d674b'
my_access_token = 'EAANoD9I4obMBAPcoZA5V7OZBQaPa3Tk7NMAT0ZBZCepdD8zZBcwMZBMHAM1zPeQiRY4Yw07rscee4LMRn9lMsJGuNZAYBA4nCYdZA6tsyL0KGTfQKIAFls3T5jul9Am6t95nbvcGXFmcFDYEyZAX2FpAuVesVGyiHuLFRKxlXfh5t6AZDZD'

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

import json
import datetime
import pandas as pd
import mysql_adactivity_save

campaign_objective = {
    'LINK_CLICKS': 'link_click',
    'POST_ENGAGEMENT': 'post_engagement', 
    'VIDEO_VIEWS': 'video_view', 
    'CONVERSIONS':'offsite_conversion.fb_pixel_purchase',
    'APP_INSTALLS': 'app_installs',
    'BRAND_AWARENESS': 'brand_awareness',
    'EVENT_RESPONSES': 'event_responses',
    'LEAD_GENERATION': 'lead_generation',
    'LOCAL_AWARENESS': 'local_awareness',
    'MESSAGES': 'messages',
    'OFFER_CLAIMS': 'offer_claims',
    'PAGE_LIKES': 'page_likes',
    'PRODUCT_CATALOG_SALES': 'product_catalog_sales',
    'REACH': 'reach',
    'ALL_CLICKS': 'clicks',
}
campaign_field = {
    'spend_cap': Campaign.Field.spend_cap,
    'objective': Campaign.Field.objective,
    'start_time': Campaign.Field.start_time,
    'stop_time': Campaign.Field.stop_time,
}
adset_field = {
    'optimization_goal': AdSet.Field.optimization_goal,
    'bid_amount': AdSet.Field.bid_amount,
    'daily_budget': AdSet.Field.daily_budget,
    'targeting': AdSet.Field.targeting,
}
campaign_insights = {
    'campaign_id': AdsInsights.Field.campaign_id,
}
adset_insights = {
    'adset_id': AdsInsights.Field.adset_id,
}
general_insights = {
    'impressions': AdsInsights.Field.impressions,
    'reach': AdsInsights.Field.reach,
    'spend': AdsInsights.Field.spend,
    'cpc': AdsInsights.Field.cpc,
    'clicks': AdsInsights.Field.clicks,
}
target_insights = {

    'actions': AdsInsights.Field.actions,
    'cost_per_actions': AdsInsights.Field.cost_per_action_type,
}
class Field:
    target_type = 'target_type'
    target = 'target'
    cost_per_target = 'cost_per_target'
    charge_type = 'charge_type'
    start_time = 'start_time'
    stop_time = 'stop_time'
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
    
class Accounts(object):
    def __init__( self, account_id ):
        self.account_id = account_id
        self.account_insights = dict()
        
    def get_account_insights( self ):
        accounts = AdAccount( self.account_id )
        params = {
            'date_preset': 'today',
        }
        insights = accounts.get_insights(
            params=params,
            fields=list( general_insights.values() )+list( target_insights.values() )
        )
        return insights[0]
    
class Campaigns(object):
    def __init__( self, campaign_id, charge_type ):
        self.campaign_id = campaign_id
        self.charge_type = charge_type
        self.campaign_insights = dict()
        self.campaign_features = dict()
        
    # Getters
    
    def get_campaign_features( self ):
        ad_campaign = Campaign( self.campaign_id )
        adcamps = ad_campaign.remote_read( fields=list(campaign_field.values()) )
        for k in list(adcamps.keys()):
            self.campaign_features.update( {k:adcamps.get(k)} )
        return self.campaign_features
    
    def get_campaign_insights( self, date_preset=None ):
        campaign = Campaign( self.campaign_id )
        params = {
            'date_preset': date_preset,
        }
        insights = campaign.get_insights(
            params=params,
            fields=list( general_insights.values() )+list( target_insights.values() )
        )
        if bool(insights):
            self.campaign_insights.update( {'cost_per_target':0} )
            self.campaign_insights.update( {'target':0} )
            try:
                for act in insights[0].get( Field.actions ):
                    if act["action_type"] == campaign_objective[ self.charge_type ]:
                        target = act["value"]
                        self.campaign_insights.update( {'target':target} )
                for act in insights[0].get( Field.cost_per_action_type ):
                     if act["action_type"] == campaign_objective[ self.charge_type ]:
                        cost_per_target = act["value"]
                        self.campaign_insights.update( {'cost_per_target':cost_per_target} )
            except:
                print('pass')
                pass
            for k in list( general_insights.keys() ):
                self.campaign_insights.update( {k: insights[0].get(k)} )
            if self.charge_type != 'ALL_CLICKS':
                self.campaign_insights.pop( Field.clicks )
                self.campaign_insights.pop( Field.cpc )
            else:
                self.campaign_insights[ 'target' ] = self.campaign_insights.pop( Field.clicks )
                self.campaign_insights[ 'cost_per_target' ] = self.campaign_insights.pop( Field.cpc )
            return self.campaign_insights

    def get_adsets( self ):
        adset_list=list()
        campaign = Campaign( self.campaign_id )
        adsets = campaign.get_ad_sets( fields = [ AdSet.Field.id ])
        ads = campaign.get_ad_sets( fields = [ AdSet.Field.id ])
        for adset in adsets:
            adset_list.append( adset.get("id") )
        return adset_list

    def get_account_id( self ):
        campaign = Campaign( self.campaign_id )
        account = campaign.get_insights(fields=[Campaign.Field.account_id])
        for acc in account:
            acc_id = acc.get( Field.account_id )
            return acc_id
    # Operator
    
    def to_campaign( self, date_preset='lifetime' ):
        self.get_campaign_features()
        self.get_campaign_insights( date_preset )
        print(self.campaign_insights)
        self.campaign_features[ Field.campaign_id ] = self.campaign_features.pop('id')
        self.campaign_features[ Field.target_type ] = self.campaign_features.pop('objective')
        self.campaign_features[ Field.start_time ] = datetime.datetime.strptime( self.campaign_features[Field.start_time],'%Y-%m-%dT%H:%M:%S+%f' )
        self.campaign_features[ Field.stop_time ] = datetime.datetime.strptime( self.campaign_features[Field.stop_time],'%Y-%m-%dT%H:%M:%S+%f' )
        self.campaign_features[ Field.period ] = ( self.campaign_features[Field.stop_time] - self.campaign_features[Field.start_time] ).days
        self.campaign_features[ Field.start_time ] = self.campaign_features[Field.start_time].strftime( '%Y-%m-%d %H:%M:%S' )
        self.campaign_features[ Field.stop_time ] = self.campaign_features[Field.stop_time].strftime( '%Y-%m-%d %H:%M:%S' )
        self.campaign_features[ Field.daily_budget ] = int( self.campaign_features[Field.spend_cap] )/self.campaign_features[Field.period]
        self.campaign = { **self.campaign_insights, **self.campaign_features }
        return self.campaign
    
class AdSets(object):
    def __init__( self, adset_id, charge_type ):
        self.adset_id = adset_id
        self.charge_type = charge_type
        self.adset_features = dict()
        self.adset_insights = dict()
        self.adset = dict()
        
    # Getters
        
    def get_adset_features( self ):
        adset = AdSet( self.adset_id )
        adsets = adset.remote_read( fields=list( adset_field.values() ) )
        for k in list(adsets.keys()):
            if k == Field.targeting:
                self.adset_features.update( { Field.age_max: adsets.get( Field.targeting ).get( Field.age_max ) } )
                self.adset_features.update( { Field.age_min: adsets.get( Field.targeting ).get( Field.age_min ) } )
                self.adset_features.update( { Field.flexible_spec: str( adsets.get( Field.targeting ).get( Field.flexible_spec ) ) } )
                self.adset_features.update( { Field.geo_locations: str( adsets.get( Field.targeting ).get( Field.geo_locations ) ) } )
            else:
                self.adset_features.update( { k:adsets.get(k) } )
        return self.adset_features
    
    def get_adset_insights( self, date_preset=None ):
        adset = AdSet( self.adset_id )
        params = {
            'date_preset': date_preset,
        }
        insights = adset.get_insights(
            params=params,
            fields=list( general_insights.values() )+list( target_insights.values() )
        )
        if bool(insights):
            self.adset_insights.update( {'target':'0'} )
            self.adset_insights.update( {'cost_per_target':'0'} )
            try:
                for act in insights[0].get( Field.actions ):
                     if act["action_type"] == campaign_objective[ self.charge_type ]:
                        target = act["value"]
                        self.adset_insights.update( {'target':target} )
                for act in insights[0].get( Field.cost_per_action_type ):
                     if act["action_type"] == campaign_objective[ self.charge_type ]:
                        cost_per_target = act["value"]
                        self.adset_insights.update( {'cost_per_target':cost_per_target} )
            except:
                pass
            finally:
                for k in list( general_insights.keys() ):
                    self.adset_insights.update( {k: insights[0].get(k)} )
            if self.charge_type != 'ALL_CLICKS':
                self.adset_insights.pop( Field.clicks )
                self.adset_insights.pop( Field.cpc )
            else:
                self.adset_insights[ 'target' ] = self.adset_insights.pop( Field.clicks )
                self.adset_insights[ 'cost_per_target' ] = self.adset_insights.pop( Field.cpc )
        else:
            self.adset_insights[ Field.target ] = '0'
            self.adset_insights[ Field.cost_per_target ] = '0'
            self.adset_insights[ Field.impressions ] = '0'
            self.adset_insights[ Field.reach ] = '0'
            self.adset_insights[ Field.spend ] = '0'
        return self.adset_insights

    # Operator
    
    def to_adset( self, date_preset=None ):
        self.get_adset_features()
        self.get_adset_insights( date_preset )
        self.adset_features[ Field.adset_id ] = self.adset_features.pop('id')
        self.adset = { **self.adset_insights, **self.adset_features }
        return self.adset

def data_collect( campaign_id, total_clicks, charge_type ):
    camp = Campaigns( campaign_id, charge_type )
    
    request_dict = {'request_time': datetime.datetime.now()}
    charge_dict = {'charge_type': charge_type}
    target_dict = {'destination': total_clicks}
    life_time_campaign_insights = camp.to_campaign( date_preset=DatePreset.lifetime )
#     print(life_time_campaign_insights)
    stop_time = datetime.datetime.strptime( life_time_campaign_insights[Field.stop_time],'%Y-%m-%d %H:%M:%S' )
    period_left = (stop_time-datetime.datetime.now()).days + 1
    charge = life_time_campaign_insights[ Field.target ]
    target_left = {'target_left': int(total_clicks) - int(charge)}
    daily_charge = {'daily_charge': int(target_left['target_left']) / period_left}
    campaign_dict = {
        **life_time_campaign_insights,
        **charge_dict,
        **target_left,
        **target_dict,
        **daily_charge,
    }
    adset_list = camp.get_adsets()
    for adset_id in adset_list:
        adset = AdSets(adset_id, charge_type)
        adset_dict = adset.to_adset(date_preset=DatePreset.today)
        adset_dict['request_time'] = datetime.datetime.now()
        adset_dict['campaign_id'] = campaign_id
        df_adset = pd.DataFrame(adset_dict, index=[0])
        mysql_adactivity_save.intoDB("adset_insights", df_adset)#insert into adset_insights
    df_camp = pd.DataFrame(campaign_dict, index=[0])
    df_camp[df_camp.columns] = df_camp[df_camp.columns].apply(pd.to_numeric, errors='ignore')
    
#     from tabulate import tabulate
#     print(tabulate(df_camp,headers=df_camp.columns, tablefmt='psql'))
    
    mysql_adactivity_save.update_campaign_target(df_camp)#update to campaign_target
    return

def make_default( campaign_id, charge_type ):
    adset_list = Campaigns(campaign_id, charge_type).get_adsets()
    mydict=dict()
    result={ "media": "Facebook", "campaign_id": campaign_id, "contents":[] }
    for adset_id in adset_list:
        adset_dict = AdSets(adset_id, charge_type).to_adset()
        if not bool(adset_dict):
            pass
        else:
            result["contents"].append(
                {
                    "pred_cpc": str( adset_dict['bid_amount'] ),
                    "pred_budget": str( adset_dict['daily_budget'] ),
                    "adset_id": str( adset_id ),
                }
            )
    if result["contents"]:
        mydict_json = json.dumps(result)
        mysql_adactivity_save.insert_default( str( campaign_id ), mydict_json, datetime.datetime.now() )
        
#     if bool(mydict):
#         mydict_json = json.dumps(mydict)
#         mysql_adactivity_save.insert_default( str( campaign_id ), mydict_json, datetime.datetime.now() ) 
    return

def main():
    start_time = datetime.datetime.now()
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    campaignid_target_dict = mysql_adactivity_save.get_campaign_target_dict()    
    for campaign_id in campaignid_target_dict:
        charge = campaignid_target_dict.get(campaign_id)
        df = mysql_adactivity_save.get_campaign_target(campaign_id)
        print(campaign_id, df['charge_type'])
        data_collect( campaign_id.astype(dtype=object), charge.iloc[0].astype(dtype=object), df['charge_type'].iloc[0] )#存資料
    print(datetime.datetime.now()-start_time)
    
if __name__ == "__main__":
    main()