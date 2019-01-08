import datetime
import pandas as pd
import mysql_adactivity_save

DATADASE = "ad_activity"
START_TIME = 'start_time'
STOP_TIME = 'stop_time'
AD_ID = 'ad_id'
ADSET_ID = 'adset_id'
CAMPAIGN_ID = 'campaign_id'
CHARGE = 'charge'
BID_AMOUNT = 'bid_amount'
REQUEST_TIME = 'request_time'
TARGET_LEFT = 'target_left'

class FacebookCampaignAdapter(object):
    def __init__(self, campaign_id):
        self.mydb = mysql_adactivity_save.connectDB( DATADASE )
        self.limit = 10000
        self.hour_per_day = 20
        self.campaign_id = campaign_id
        self.request_time = datetime.datetime.now()
        self.time_progress = ( self.request_time.hour + 1 ) / self.hour_per_day
        
    def get_df(self):
        self.df_camp = pd.read_sql( "SELECT * FROM campaign_target WHERE campaign_id=%s" %( self.campaign_id ), con=self.mydb )
        self.df_ad = pd.read_sql( "SELECT * FROM ad_insights where campaign_id = %s ORDER BY request_time DESC LIMIT %s" %( self.campaign_id, self.limit ), con=self.mydb )
        return
    
    def get_campaign_days_left(self):
        self.campaign_days_left = ( self.df_camp[ STOP_TIME ].iloc[0] - self.request_time ).days
        return self.campaign_days_left
    
    def get_campaign_days(self):
        self.campaign_days = ( self.df_camp[ STOP_TIME ].iloc[0] - self.df_camp[ START_TIME ].iloc[0] ).days
        return self.campaign_days
    
    def get_campaign_performance(self):
        df_ads = pd.read_sql( "SELECT * FROM ad_insights where campaign_id = %s ORDER BY request_time desc limit %s" %( self.campaign_id, self.limit ), con=self.mydb )
        ad_id_list = df_ads[ AD_ID ].unique()
        dfs = pd.DataFrame(columns=[ ADSET_ID, CHARGE ])
#         for ad_id in tqdm(ad_id_list):
        for ad_id in ad_id_list:
            ad_id = ad_id.astype(dtype=object)
            df_ad = df_ads[df_ads.ad_id==ad_id]
            adset_id = df_ad[ ADSET_ID ].iloc[0]
            df = df_ad[[ CHARGE, REQUEST_TIME ]][df_ad.request_time.dt.date==self.request_time]
            dfs = pd.concat([dfs, df], axis=0, sort=False)
        dfs = dfs.sort_values(by=[ CHARGE ], ascending=False).reset_index(drop=True)
        self.campaign_performance = dfs[ CHARGE ].sum()
        return self.campaign_performance
    
    def get_campaign_target(self):
        self.campaign_target = self.df_camp[ TARGET_LEFT ].iloc[0].astype(dtype=object)
        return self.campaign_target
    
    def get_campaign_day_target(self):
        self.campaign_day_target = self.campaign_target / self.campaign_days_left
        return self.campaign_day_target
    
    def get_adset_list(self):
        df_ad = pd.read_sql( "SELECT * FROM ad_insights where campaign_id = %s ORDER BY request_time desc limit %s" %( self.campaign_id, self.limit ), con=self.mydb )
        return df_ad[ ADSET_ID ].unique()
    
    def retrieve_campaign_attribute(self):
        self.get_df()
        self.get_campaign_days_left()
        self.get_campaign_days()
        self.get_campaign_performance()
        self.get_campaign_target()
        self.get_campaign_day_target()
        return

class FacebookAdSetAdapter(FacebookCampaignAdapter):
    def __init__(self, adset_id, fb):
        self.mydb = mysql_adactivity_save.connectDB( DATADASE )
        self.adset_id = adset_id
        self.fb = fb
        
    def init_campaign(self, fb):
        self.time_progress = fb.time_progress
        self.limit = fb.limit
        self.hour_per_day = fb.hour_per_day
        self.request_time = fb.request_time
        self.df_camp = fb.df_camp
        self.df_ad = fb.df_ad
        self.campaign_days_left = fb.campaign_days_left
        self.campaign_days = fb.campaign_days
        self.campaign_performance = fb.campaign_performance
        self.campaign_target = fb.campaign_target
        self.campaign_day_target = fb.campaign_day_target
        return
    
    def get_campaign_id(self):
        df_ad = pd.read_sql( "SELECT * FROM ad_insights WHERE adset_id = %s ORDER BY request_time DESC LIMIT 1" %( self.adset_id ), con=self.mydb )
        self.campaign_id = df_ad[ CAMPAIGN_ID ].iloc[0].astype(dtype=object)
        return self.campaign_id
    
    def get_adset_day_target(self):
        adset_num = len( self.df_ad[ ADSET_ID ].unique() )
        self.adset_day_target = self.fb.campaign_day_target / adset_num
        return self.adset_day_target
    
    def get_adset_performance(self):
        df_ad = pd.read_sql( "SELECT * FROM ad_insights WHERE adset_id = %s ORDER BY request_time DESC LIMIT 1" %( self.adset_id ), con=self.mydb )
        self.adset_performance = df_ad[ CHARGE ].iloc[0]
        return self.adset_performance
    
    def get_init_bid(self):
        df_adset = pd.read_sql( "SELECT * FROM adset_insights WHERE adset_id=%s LIMIT 1" %( self.adset_id ), con=self.mydb )
        self.init_bid = df_adset[ BID_AMOUNT ].iloc[0]
        return self.init_bid
    
    def get_last_bid(self):
        df_adset = pd.read_sql( "SELECT * FROM adset_insights WHERE adset_id=%s ORDER BY request_time DESC LIMIT 1" %( self.adset_id ), con=self.mydb )
        self.last_bid = df_adset[ BID_AMOUNT ].iloc[0].astype(dtype=object)    
        return self.init_bid
    
    def get_adset_time_target(self):
        self.adset_time_target = self.adset_day_target * self.time_progress
        return self.adset_time_target
    
    def get_adset_progress(self):
        self.adset_progress = self.adset_performance / self.adset_time_target
        return self.adset_progress
    
    def retrieve_adset_attribute(self):
        self.get_campaign_id()
        self.init_campaign(self.fb)
        self.get_df()
        self.get_adset_day_target()
        self.get_adset_performance()
        self.get_init_bid()
        self.get_last_bid()
        self.get_adset_time_target()
        self.get_adset_progress()
        return

def main():
    start_time = datetime.datetime.now()
    campaignid_target_dict = mysql_adactivity_save.get_campaign_target_dict()
    for campaign_id in campaignid_target_dict:
        fb = FacebookCampaignAdapter( campaign_id )
        fb.retrieve_campaign_attribute()
        adset_list = fb.get_adset_list()
        for adset in adset_list:
            s = FacebookAdSetAdapter( adset, fb )
            s.retrieve_adset_attribute()
    print(datetime.datetime.now()-start_time)
if __name__=='__main__':
    main()
    
    