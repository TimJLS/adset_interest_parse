import math
import numpy as np

ADSET_ID = 'adset_id'
INIT_BID = 'init_bid'
LAST_BID = 'last_bid'
ADSET_PROGRESS = 'adset_progress'
CAMPAIGN_PROGRESS = 'campaign_progress'
BID = 'bid'

CENTER = 1
WIDTH = 10
BID_RANGE = 0.8

def normalized_sigmoid_fkt(center, width, progress):
    s= 1/( 1 + np.exp( width * ( progress-center ) ) )
    return s

def adjust(**status):
    adset_id = status.get(ADSET_ID)
    init_bid = status.get(INIT_BID)
    last_bid = status.get(LAST_BID)
    adset_progress = status.get(ADSET_PROGRESS)
    campaign_progress = status.get(CAMPAIGN_PROGRESS)
    if adset_progress > 1 and campaign_progress > 1:
        bid = math.ceil(init_bid)
    elif adset_progress > 1 and campaign_progress < 1:
        bid = last_bid
    else:
        bid = init_bid + BID_RANGE*init_bid*( normalized_sigmoid_fkt(CENTER, WIDTH, adset_progress) - 0.5 )
        bid = bid.astype(dtype=object)
    return { ADSET_ID:adset_id, BID:bid }

if __name__=='__main__':
    status = {'campaign_progress': -0.0, 'adset_id': 23843355587230564, 'init_bid': 11, 'adset_progress': -0.7373064458048254, 'last_bid': 15}

    adjust(**status)