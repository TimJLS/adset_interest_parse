import database_controller
import facebook_adset_controller
import adgeek_permission

def main():
    global database_fb
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    campaign_not_opted_list = database_fb.get_performance_campaign().to_dict('records')

    print('df_not_opted len:', len(campaign_not_opted_list))
    print(campaign_not_opted_list)
    for campaign in campaign_not_opted_list:

        facebook_adset_controller.fast_test_remove_copy_string(campaign['account_id'], campaign['campaign_id'])
        
if __name__=='__main__':
    main()