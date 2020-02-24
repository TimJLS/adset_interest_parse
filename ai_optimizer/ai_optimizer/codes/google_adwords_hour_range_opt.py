import pandas as pd
import numpy as np
import datetime
import traceback

import database_controller
import google_adwords_report_generator as generator
import google_adwords_controller as controller

def generate_report(campaign_id, data_preset):
    """
    This method will return every single weekday's hour-range performance during the given data-preset.
    If the data-preset is one single day (today/yesterday), it will still return other weekdays' records, 
    but with empty data points.
    """
    # Initiate report generator
    camp_rpt_gen = generator.AdScheduleReportGenerator(campaign_id, 'gdn')
    
    # Add segment condition of days and get the campaign report
    params = {'breakdowns': ['day']}
    insight = camp_rpt_gen.get_insights(data_preset, params)
    report_df = pd.DataFrame(insight)
    
    return report_df


def get_exact_yesterday_report(report_df):
    """
    This method is to identify the valid data record only for yesterday.
    """
    yesterday_idx = (datetime.date.today() - datetime.timedelta(1)).isoweekday()
    weekday_map = dict(zip([x for x in range(1,8)], ['mon','tue','wed','thr','fri','sat','sun']))
    last_weekday = weekday_map[yesterday_idx]
    yesterday_rpt = report_df[report_df['weekofday']==last_weekday]
    
    return yesterday_rpt
    

def set_opt_flag(campaign, yesterday_rpt, campaign_type):
    """
    Set the opt_flag to indicate whether to execute the optimization or not.
    
    First, check if the number of hour range is more than 1, 
    and every hour range's goal type (clicks/conversions) is not 0.
    
    Then, filter off the campaign if every hour range's goal type is higher or lower than kpi.
    Return a dictionary indicate which hour ranges to raise or reduce the bid modifier.
    ------------------------------------------------------------------------
    parameters
    
    campaign: A dictionary of the details of the campaign from the campaign_target table. Can be gained 
    by operating methods of database_controller.
    
    yesterday_rpt: A dataframe of yesterday performance.
    
    campaign_type: A string of cpc or cpa
    """
    opt_flag = False
    hour_range_adjustment = {'raise_hr': None, 'reduction_hr': None}  
    campaign_type_map = {
        'cpc': {'goal_type': 'clicks', 'cost_type': 'cost_per_click'}, 
        'cpa': {'goal_type': 'conversions', 'cost_type': 'cost_per_conversion'}
    }
    
    mapper = campaign_type_map[campaign_type]
    goal_type = mapper['goal_type']
    cost_type = mapper['cost_type']
        
    if (len(yesterday_rpt) > 1) and (all(yesterday_rpt[goal_type])):
        kpi = campaign['ai_spend_cap'] / campaign['destination']  
        
        if not all(cpx > kpi for cpx in yesterday_rpt[cost_type]):
            if not all(cpx < kpi for cpx in yesterday_rpt[cost_type]):
                opt_flag = True
                hour_range_adjustment['reduction_hr'] = (
                    yesterday_rpt[yesterday_rpt[cost_type] > kpi]['starthour']
                )
                
                if campaign_type == 'cpa':
                    hour_range_adjustment['raise_hr'] = (
                        yesterday_rpt[yesterday_rpt[cost_type] < kpi]['starthour']
                    )
                
    return (opt_flag, hour_range_adjustment)


def optimize_hour_range(customer_id, campaign_id, hour_range_adjustment):
    """
    Update bid modifier value based on last day performances of different hour ranges
    Range of modification: 0.1 - 1.9
    If the bid modifier is 0.0, do not make any change.
    """
    
    # Get bid modifier settings
    camp_serv_container = controller.CampaignServiceContainer(customer_id)
    camp_controller = controller.Campaign(camp_serv_container, campaign_id)
    schedule = camp_controller.ad_schedules.retrieve()

    # Transfer today's weekday name for matching report and setting
    weekday_name_map = dict(
        zip([x for x in range(1,8)], ['MONDAY','TUESDAY','WEDNESDAY','THURSDAY','FRIDAY','SATURDAY','SUNDAY'])
    )
    weekday_of_today = weekday_name_map[datetime.date.today().isoweekday()]

    # Update new bid modifier values
    date_range_settings = schedule[weekday_of_today]
    for setting in date_range_settings:
        try:
            start_hour = setting['criterion']['startHour']
            end_hour = setting['criterion']['endHour']
            criterion_id = setting['criterion']['id']

            if setting['bidModifier'] == 0.0:
                print('Campaign:', campaign_id, 'Start:', start_hour, 
                      ' Has not been changed cause bidModifier is -100%')

            else:
                if not setting['bidModifier']:
                    setting['bidModifier'] = 1.0

                if start_hour in hour_range_adjustment['reduction_hr']:
                    if setting['bidModifier'] > 0.1:
                        bid_modifier = round((setting['bidModifier'] - 0.1), 1)
                        camp_controller.ad_schedules.operand['criterion']['id'] = criterion_id
                        response = camp_controller.ad_schedules.update(weekday_of_today, start_hour, end_hour, 
                                                               bid_modifier, criterion_id)

                        print(response)

                elif start_hour in hour_range_adjustment['raise_hr']:
                    if setting['bidModifier'] < 1.9:
                        bid_modifier = round((setting['bidModifier'] + 0.1), 1)                
                        camp_controller.ad_schedules.operand['criterion']['id'] = criterion_id
                        response = camp_controller.ad_schedules.update(weekday_of_today, start_hour, end_hour, 
                                                                       bid_modifier, criterion_id)

                        print(response)
                
        except:
            print('Unexpected error')
            print('Campaign_id:', setting['campaignId'])
            traceback.print_exc()

                                                            
if __name__ == '__main__':
    db = database_controller.Database
    database_gdn = database_controller.GDN( db )
    branding_campaign_list = database_gdn.get_branding_campaign().to_dict('records')
    branding_campaign_list = [campaign for campaign in branding_campaign_list if eval(campaign['is_hour_adjust'])]

    performance_campaign_list = database_gdn.get_performance_campaign().to_dict('records')
    performance_campaign_list = [campaign for campaign in performance_campaign_list if eval(campaign['is_hour_adjust'])]


    for campaign in branding_campaign_list + performance_campaign_list:
        # Get the whole date-range report accross every weekday
        report_df = generate_report(campaign['campaign_id'], generator.DatePreset.yesterday)

        # Narrow down to only yesterday report
        yesterday_rpt = get_exact_yesterday_report(report_df)
        
        if campaign in branding_campaign_list:
            campaign_type = 'cpc'

        elif campaign in performance_campaign_list:
            campaign_type = 'cpa' 
            
        # Check whether to run the optimization job
        opt_flag, hour_range_adjustment = set_opt_flag(campaign, yesterday_rpt, campaign_type)
        
        if opt_flag:
            # Execute optimization only if yesterday has more than 1 hour-range settings
            optimize_hour_range(campaign['customer_id'], campaign['campaign_id'], hour_range_adjustment)