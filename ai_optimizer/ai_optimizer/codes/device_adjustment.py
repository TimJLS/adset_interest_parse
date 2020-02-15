import pandas as pd
import numpy as np
import datetime

from googleads import adwords
import database_controller
import google_adwords_report_generator as generator
from google_adwords_report_generator import DatePreset
import adgeek_permission as permission
import google_adwords_controller as controller

AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
adwords_client = adwords.AdWordsClient.LoadFromStorage(AUTH_FILE_PATH)


def generate_report(campaign_id, data_preset):
    # Initiate report generator
    ad_report_gen = generator.AdGroupReportGenerator(campaign_id, 'gdn')

    # Add segment condition of devices and get the campaign report
    params = {'breakdowns': 'device'}
    insight = ad_report_gen.get_insights(data_preset, params)
    report_df = pd.DataFrame(insight)
    
    return report_df


def adjust_bid_modifier_cpc(adgroup, report_df):
    '''
    adgroup: A google_adwords_controller.Adgroup object
    report_df: A dataframe generated from generate_report method
    '''
    bid_modifier_list = adgroup.bid_modifier.retrieve()
    adg_id = adgroup.ad_group_id
    adgroup_report = report_df[report_df['adgroup_id']==adg_id][['device', 'spend', 'clicks']]
    mobile_rpt = adgroup_report[adgroup_report['device']=='Mobile devices with full browsers']
    not_mobile_rpt = adgroup_report[adgroup_report['device']!='Mobile devices with full browsers']
    
    # Check if clicks is 0:
    mobile_clicks = mobile_rpt['clicks'].sum()
    not_mobile_clicks = not_mobile_rpt['clicks'].sum()
    operation = {
                'operator': 'ADD',
                'operand': None
            }
    
    if (mobile_clicks == 0) or (not_mobile_clicks == 0):
        pass
    
    else:      
        mobile_cpc = mobile_rpt['spend'].sum() / mobile_clicks
        not_mobile_cpc = not_mobile_rpt['spend'].sum() / not_mobile_clicks
        
        if mobile_cpc > not_mobile_cpc:
            for bid_modifier in bid_modifier_list:
                if bid_modifier['criterion']['platformName'] == 'HighEndMobile':
                    if not bid_modifier['bidModifier']:
                        bid_modifier['bidModifier'] = 1.0
                    if (bid_modifier['bidModifier'] != 0.0) and (bid_modifier['bidModifier'] != 0.1):
                        bid_modifier['bidModifier'] = round((bid_modifier['bidModifier'] - 0.1), 1)
                        operation['operand'] = bid_modifier
                        adgroup.service_container.service_bid_modifier.mutate(operation)
                        
                        
        elif mobile_cpc < not_mobile_cpc:
            for bid_modifier in bid_modifier_list:
                if bid_modifier['criterion']['platformName'] != 'HighEndMobile':
                    if not bid_modifier['bidModifier']:
                        bid_modifier['bidModifier'] = 1.0
                    if (bid_modifier['bidModifier'] != 0.0) and (bid_modifier['bidModifier'] != 0.1):
                        bid_modifier['bidModifier'] = round((bid_modifier['bidModifier'] - 0.1), 1)
                        operation['operand'] = bid_modifier
                        adgroup.service_container.service_bid_modifier.mutate(operation)
        
    print(bid_modifier_list)
    print('--------next--------')
    print()
    
    return operation


def adjust_bid_modifier_cpa(adgroup, report_df):
    '''
    adgroup: A google_adwords_controller.Adgroup object
    report_df: A dataframe generated from generate_report method
    '''
    bid_modifier_list = adgroup.bid_modifier.retrieve()
    adg_id = adgroup.ad_group_id
    adgroup_report = report_df[report_df['adgroup_id']==adg_id][['device', 'spend', 'clicks']]
    mobile_rpt = adgroup_report[adgroup_report['device']=='Mobile devices with full browsers']
    not_mobile_rpt = adgroup_report[adgroup_report['device']!='Mobile devices with full browsers']
    
    # Check if clicks is 0:
    mobile_conversions = mobile_rpt['conversions'].sum()
    not_mobile_conversions = not_mobile_rpt['conversions'].sum()
    operation = {
                'operator': 'ADD',
                'operand': None
            }
    
    if (conversions == 0) or (not_mobile_conversions == 0):
        pass
    
    else:      
        mobile_cpa = mobile_rpt['spend'].sum() / mobile_conversions
        not_mobile_cpa = not_mobile_rpt['spend'].sum() / not_mobile_conversions
        
        if mobile_cpa > not_mobile_cpa:
            for bid_modifier in bid_modifier_list:
                if bid_modifier['criterion']['platformName'] == 'HighEndMobile':
                    if not bid_modifier['bidModifier']:
                        bid_modifier['bidModifier'] = 1.0
                        
                    if (bid_modifier['bidModifier'] != 0.0) and (bid_modifier['bidModifier'] != 0.1):
                        bid_modifier['bidModifier'] = round((bid_modifier['bidModifier'] - 0.1), 1)
                        operation['operand'] = bid_modifier
                        adgroup.service_container.service_bid_modifier.mutate(operation)
                
                if bid_modifier['criterion']['platformName'] != 'HighEndMobile':
                    if not bid_modifier['bidModifier']:
                        bid_modifier['bidModifier'] = 1.0
                        
                    if (bid_modifier['bidModifier'] != 0.0) and (bid_modifier['bidModifier'] <= 1.8):
                        bid_modifier['bidModifier'] = round((bid_modifier['bidModifier'] + 0.1), 1)
                        operation['operand'] = bid_modifier
                        adgroup.service_container.service_bid_modifier.mutate(operation)
                        
                        
        elif mobile_cpa < not_mobile_cpa:
            for bid_modifier in bid_modifier_list:
                if bid_modifier['criterion']['platformName'] != 'HighEndMobile':
                    if not bid_modifier['bidModifier']:
                        bid_modifier['bidModifier'] = 1.0
                        
                    if (bid_modifier['bidModifier'] != 0.0) and (bid_modifier['bidModifier'] != 0.1):
                        bid_modifier['bidModifier'] = round((bid_modifier['bidModifier'] - 0.1), 1)
                        operation['operand'] = bid_modifier
                        adgroup.service_container.service_bid_modifier.mutate(operation)
                
                if bid_modifier['criterion']['platformName'] == 'HighEndMobile':
                    if not bid_modifier['bidModifier']:
                        bid_modifier['bidModifier'] = 1.0
                    
                    if (bid_modifier['bidModifier'] != 0.0) and (bid_modifier['bidModifier'] <= 1.8):
                        bid_modifier['bidModifier'] = round((bid_modifier['bidModifier'] + 0.1), 1)
                        operation['operand'] = bid_modifier
                        adgroup.service_container.service_bid_modifier.mutate(operation)
        
    print(bid_modifier_list)
    print('next')
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
    
    for campaign in branding_campaign_list:
        # Generate the report
        report_today = generate_report(campaign['campaign_id'], DatePreset.today)
        report_yesterday = generate_report(campaign['campaign_id'], DatePreset.yesterday)
        report_df = report_today.append(report_yesterday)[['adgroup_id', 'device', 'clicks', 'spend']]
        # Get current settings
        adgroup_container = controller.AdGroupServiceContainer(campaign['customer_id'])
        adgroup_controller = controller.Campaign(adgroup_container, campaign['campaign_id'])

        for adgroup in adgroup_controller.get_ad_groups():
            adjust_bid_modifier_cpc(adgroup, report_df)
            
    for campaign in performance_campaign_list:
        # Generate the report
        report_today = generate_report(campaign['campaign_id'], DatePreset.today)
        report_yesterday = generate_report(campaign['campaign_id'], DatePreset.yesterday)
        report_df = report_today.append(report_yesterday)[['adgroup_id', 'device', 'conversions', 'spend']]
        
        # Get current settings
        adgroup_container = controller.AdGroupServiceContainer(campaign['customer_id'])
        adgroup_controller = controller.Campaign(adgroup_container, campaign['campaign_id'])

        for adgroup in adgroup_controller.get_ad_groups():
            adjust_bid_modifier_cpa(adgroup, report_df)
    
    
    