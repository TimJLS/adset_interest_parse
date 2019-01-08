class FacebookAdapter(object):
    def __init__(self, campaign_id):
        self.campaign_id=campaign_id
    def time_progress(hour_per_day=20, request_time=datetime.datetime.now()):
        return ( request_time.hour + 1 ) / hour_per_day
