import pandas as pd
import numpy as np
import datetime

from googleads import adwords
import database_controller
import google_adwords_report_generator as generator
from google_adwords_report_generator import DatePreset
import adgeek_permission as permission
import google_adwords_controller as controller

# AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
# adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)


def generate_report(campaign_id, data_preset):
    # Initiate report generator
    ad_report_gen = generator.AdGroupReportGenerator(campaign_id, 'gdn')

    # Add segment condition of devices and get the campaign report
    params = {'breakdowns': ['device']}
    insight = ad_report_gen.get_insights(data_preset, params)
    report_df = pd.DataFrame(insight)
    
    return report_df


def adjust_bid_modifier(campaign, adgroup, report_df, campaign_type):
    '''
    Parameters:
    
    campaign: A dictionary of the details of the campaign from the campaign_target table. Can be gained 
    by operating methods of database_controller.
    adgroup: A google_adwords_controller.Adgroup object
    report_df: A dataframe generated from generate_report method
    campaign_type: A string of cpc or cpa
    '''
    bid_modifier_list = adgroup.bid_modifier.retrieve()
    adg_id = adgroup.ad_group_id
    adgroup_report = report_df[report_df['adgroup_id']==adg_id][['device', 'spend', 'clicks', 'conversions']]
    mobile_rpt = adgroup_report[adgroup_report['device']=='Mobile devices with full browsers']
    non_mobile_rpt = adgroup_report[adgroup_report['device']!='Mobile devices with full browsers']
    
    campaign_type_map = {
        'cpc': 'clicks', 
        'cpa': 'conversions'
    }
    goal_type = campaign_type_map[campaign_type]
    
    # Check if one of the goals is 0:
    mobile_goal = mobile_rpt[goal_type].sum()
    non_mobile_goal = non_mobile_rpt[goal_type].sum()
    
    operation = {
                'operator': 'ADD',
                'operand': None
            }
    
    if (mobile_goal == 0) or (non_mobile_goal == 0):
        print('One of the device categories (mobile/non-mobile) has 0 performace')
    
    else:
        mobile_cpx = mobile_rpt['spend'].sum() / mobile_goal
        non_mobile_cpx = non_mobile_rpt['spend'].sum() / non_mobile_goal
        kpi = campaign['ai_spend_cap'] / campaign['destination']
        if (mobile_cpx > kpi and non_mobile_cpx > kpi) or (mobile_cpx < kpi and non_mobile_cpx < kpi):
            print('Both mobile and non-mobile category exceed or under the kpi')
        
        else:
            for bid_mod_dict in bid_modifier_list:
                if bid_mod_dict['bidModifier'] == 0.0:
                    pass
                else:
                    if not bid_mod_dict['bidModifier']:
                        bid_mod_dict['bidModifier'] = 1.0
                    if mobile_cpx > non_mobile_cpx:
                        if bid_mod_dict['criterion']['platformName'] == 'HighEndMobile':
                            if bid_mod_dict['bidModifier'] > 0.1:
                                bid_mod_dict['bidModifier'] = round((bid_mod_dict['bidModifier'] - 0.1), 1)
                                operation['operand'] = bid_mod_dict
                                adgroup.service_container.service_bid_modifier.mutate(operation)
                        else:
                            if campaign_type == 'cpa':
                                if bid_mod_dict['bidModifier'] < 1.9:
                                    bid_mod_dict['bidModifier'] = round((bid_mod_dict['bidModifier'] + 0.1), 1)
                                    operation['operand'] = bid_mod_dict
                                    adgroup.service_container.service_bid_modifier.mutate(operation)
                    
                    elif mobile_cpx < non_mobile_cpx:
                        if bid_mod_dict['criterion']['platformName'] != 'HighEndMobile':
                            if bid_mod_dict['bidModifier'] > 0.1:
                                bid_mod_dict['bidModifier'] = round((bid_mod_dict['bidModifier'] - 0.1), 1)
                                operation['operand'] = bid_mod_dict
                                adgroup.service_container.service_bid_modifier.mutate(operation)
                        else:
                            if campaign_type == 'cpa':
                                if bid_mod_dict['bidModifier'] < 1.9:
                                    bid_mod_dict['bidModifier'] = round((bid_mod_dict['bidModifier'] + 0.1), 1)
                                    operation['operand'] = bid_mod_dict
                                    adgroup.service_container.service_bid_modifier.mutate(operation)
    
    print(bid_modifier_list)
    print('--------next--------')
    print()
    
    return operation


if __name__ == '__main__':
    # Get the to-be-processed campaigns
    db = database_controller.Database
    database_gdn = database_controller.GDN( db )
    branding_campaign_list = database_gdn.get_branding_campaign().to_dict('records')
    branding_campaign_list = [campaign for campaign in branding_campaign_list if eval(campaign['is_device_pro_rata'])]
    
    performance_campaign_list = database_gdn.get_performance_campaign().to_dict('records')
    performance_campaign_list = [campaign for campaign in performance_campaign_list if eval(campaign['is_device_pro_rata'])]
    
    for campaign in branding_campaign_list + performance_campaign_list:
        # Generate the report
        report_today = generate_report(campaign['campaign_id'], DatePreset.today)
        report_yesterday = generate_report(campaign['campaign_id'], DatePreset.yesterday)
        report_df = report_today.append(report_yesterday)
        report_df = report_df[report_df['status']=='enabled']
        
        # Get current settings
        adgroup_container = controller.AdGroupServiceContainer(campaign['customer_id'])
        adgroup_controller = controller.Campaign(adgroup_container, campaign['campaign_id'])
        
        if campaign in branding_campaign_list:
            campaign_type = 'cpc'

        elif campaign in performance_campaign_list:
            campaign_type = 'cpa'
        
        for adgroup in adgroup_controller.get_ad_groups():
            adjust_bid_modifier(campaign, adgroup, report_df, campaign_type)
            
    