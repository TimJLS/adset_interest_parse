
def bid_adjust( campaign_id ):
    '''
    time_progress
    campaign_id, campaign_days, campaign_days_left, campaign_performance, campaign_target, 
    adset_id, adset_num, adset_target, adset_time_target, adset_performance, adset_progress
    ad_id_list, init_cpc
    '''
    mydb = mysql_adactivity_save.connectDB("ad_activity")
    request_time = datetime.datetime.now()
    time_progress = ( request_time.hour + 1 ) / HOUR_PER_DAY

    df_camp = pd.read_sql( "SELECT * FROM campaign_target where campaign_id=%s" %( campaign_id ), con=mydb )
    df_ad = pd.read_sql( "SELECT * FROM ad_insights where campaign_id = %s ORDER BY request_time desc limit %s" %( campaign_id, LIMIT ), con=mydb )
    ad_id_list = df_ad['ad_id'].unique()
    result_dict=dict()
    df_ad=df_ad[df_ad.charge_cpc!=0]
    adset_num = len( df_ad['adset_id'].unique() )
    campaign_days = ( df_camp['stop_time'].iloc[0] - df_camp['start_time'].iloc[0] ).days
    campaign_days_left = ( df_camp['stop_time'].iloc[0] - request_time ).days
    dfs = pd.DataFrame(columns=['adset_id', 'charge'])
    for ad_id in ad_id_list:
        ad_id = ad_id.astype(dtype=object)
        df_ad = pd.read_sql( "SELECT * FROM ad_insights where ad_id=%s ORDER BY request_time DESC LIMIT 1" %( ad_id ), con=mydb )
        adset_id = df_ad['adset_id'].iloc[0]
        df = selection.performance_sort( adset_id )
        dfs = pd.concat([dfs, df], axis=0, sort=False)
    dfs = dfs.sort_values(by=['charge'], ascending=False).reset_index(drop=True)
    campaign_performance = dfs['charge'].sum()
    campaign_target = df_camp['target_left'].iloc[0]
#     campaign_time_target = (campaign_target-campaign_performance) * time_progress
    campaign_time_target = campaign_target / campaign_days_left# * time_progress
    adset_target = campaign_target / campaign_days_left / adset_num
    for ad_id in ad_id_list:
        ad_id = ad_id.astype(dtype=object)
        center = 1
        width = 5
        df_ad = pd.read_sql( "SELECT * FROM ad_insights where ad_id=%s ORDER BY request_time DESC LIMIT 1" %( ad_id ), con=mydb )
        adset_id = df_ad['adset_id'].iloc[0]
        
        adset_performance = df_ad['charge'].iloc[0]

        df_adset = pd.read_sql( "SELECT * FROM adset_insights where adset_id=%s LIMIT 1" %( adset_id ), con=mydb )
        init_cpc = df_adset['bid_amount'].iloc[0]
        adset_time_target = adset_target * time_progress
        adset_progress = adset_performance / adset_time_target
                
        if adset_performance > adset_time_target and campaign_performance < campaign_time_target:
            df_adset = pd.read_sql( "SELECT * FROM adset_insights where adset_id=%s ORDER BY request_time DESC LIMIT 1" %( adset_id ), con=mydb )
            bid = df_adset['bid_amount'].iloc[0].astype(dtype=object)
        elif adset_performance > adset_time_target and campaign_performance > campaign_time_target:
            bid = math.ceil(init_cpc) # Keep Initail Bid
        else:
            bid = init_cpc + BID_RANGE*init_cpc*( normalized_sigmoid_fkt(center, width, adset_progress) - 0.5 )
            bid = bid.astype(dtype=object)

        ad_dict = {'ad_id':ad_id, 'request_time':datetime.datetime.now(), 'next_cpc':math.ceil(bid),
          PRED_CPC:bid, PRED_BUDGET: df_adset['daily_budget'].iloc[0].astype(dtype=object), DECIDE_TYPE: 'Revive' }
        df_ad = pd.DataFrame(ad_dict, index=[0])
        
        result_dict[str(ad_id)] = { PRED_CPC: int(bid), PRED_BUDGET: 10000, REASONS: "collecting data, settings no change.",
                               DECIDE_TYPE: 'Revive', STATUS: True, ADSET: str(adset_id) }
       
        table = 'pred'
        mysql_adactivity_save.intoDB(table, df_ad)
        mysql_adactivity_save.update_bidcap(ad_id, bid)
    mydict_json = json.dumps(result_dict)
    mysql_adactivity_save.insert_result( campaign_id, mydict_json, datetime.datetime.now() )
    return
def main():
    bid_adjust( campaign_id.astype(dtype=object) )
    return
if __name__ == "__main__":
    main()