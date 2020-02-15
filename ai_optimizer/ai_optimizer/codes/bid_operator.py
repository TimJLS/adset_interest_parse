import math
import numpy as np
import pandas as pd
import copy
# import ai_engine_db
import database_controller
ADAPTER = {
    "Amobee":{
        "adset_id":"package_id",
        "campaign_id":"ioid",
        "adset_progress":"package_progress",
        "campaign_progress":"io_progress"
    },
    "Facebook":{
        "adset_id":"adset_id",
        "campaign_id":"campaign_id",
        "adset_progress":"adset_progress",
        "campaign_progress":"campaign_progress"
    },
    "GDN":{
        "adset_id":"adgroup_id",
        "campaign_id":"campaign_id",
        "adset_progress":"adgroup_progress",
        "campaign_progress":"campaign_progress"
    },
    "GSN":{
        "adset_id":"keyword_id",
        "campaign_id":"campaign_id",
        "adset_progress":"keyword_progress",
        "campaign_progress":"campaign_progress"
    }
}

BID = 'bid'
INIT_BID = 'init_bid'
LAST_BID = 'last_bid'

CENTER = 1
WIDTH = 10
BID_RANGE = 0.8

ai_engine_db = database_controller.AIComputation(database_controller.Database)

def reverse_bid_amount(bid_amount):
    init_bid = bid_amount / ( BID_RANGE * ( normalized_sigmoid_fkt(CENTER, WIDTH, 0) - 0.5 ) + 1 )
    return init_bid

def revert_bid_amount(bid_amount):
    init_bid = bid_amount * ( BID_RANGE * ( normalized_sigmoid_fkt(CENTER, WIDTH, 0) - 0.5 ) + 1 )
    return init_bid

def normalized_sigmoid_fkt(center, width, progress):
    s= 1/( 1 + np.exp( width * ( progress-center ) ) )
    return s

def adjust(media, **status):
#     adset_id = status.get(ADSET_ID)
    init_bid = status.get(INIT_BID)
    last_bid = status.get(LAST_BID)
    ADSET_PROGRESS = ADAPTER[media].get("adset_progress")
    CAMPAIGN_PROGRESS = ADAPTER[media].get("campaign_progress")
    ADSET_ID = ADAPTER[media].get("adset_id")
    
    adset_progress = status.get(ADSET_PROGRESS)
    campaign_progress = status.get(CAMPAIGN_PROGRESS)

    if adset_progress > 1 and campaign_progress > 1:
        bid = math.ceil(init_bid)
    elif adset_progress > 1 and campaign_progress < 1:
        bid = last_bid
    else:
#         init_bid = reverse_bid_amount(init_bid)
        bid = init_bid + BID_RANGE*init_bid*( normalized_sigmoid_fkt(CENTER, WIDTH, adset_progress) - 0.5 )
        bid = bid.astype(dtype=object)
    if not str(adset_progress).split(".")[0].isdigit():
        bid = init_bid
    status.update( {
        'media': media,
        'bid': bid
    } )
    status['id'] = status.pop( ADAPTER[media]['adset_id'] )
    status['campaign_progress'] = status.pop( ADAPTER[media]['campaign_progress'] ).astype(object)
    status['adset_progress'] = status.pop( ADAPTER[media]['adset_progress'] ).astype(object)
    df_status = pd.DataFrame( status, index=[0] )
#     ai_engine_db.insert( "bidding_computation", status)
    return { ADAPTER[media].get("adset_id"): status['id'], BID: np.round(bid, 2) }
    return { ADSET_ID:adset_id, BID:bid }

if __name__=='__main__':
#     status = {'campaign_progress': -0.0, 'adset_id': 23843355587230564, 'init_bid': 11, 'adset_progress': -0.7373064458048254, 'last_bid': 15}
#     media = "Facebook"
#     status = {'package_progress': 0.0, 'io_progress': 0.0, 'package_id': 1605818545, 'last_bid': 450, 'init_bid': 450}
#     media = 'Amobee'
    adjust(media, **status)