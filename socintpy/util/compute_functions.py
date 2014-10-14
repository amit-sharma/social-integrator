import time, datetime
import socintpy.util.utils as utils

def test_influence(la):
    split_timestamp = int(time.mktime(datetime.datetime.strptime("2013/01/01", "%Y/%m/%d").timetuple()))
    time_diff = 86400
    influence_tuples = la.estimate_influencer_effect(1, split_timestamp, time_diff,
                                                     control_divider=0.1, 
                                                     selection_method="random",
                                                     klim=5)
    mean_fr_influence = utils.mean_sd([v[5] for v in influence_tuples]) 
    mean_nonfr_influence = utils.mean_sd([v[6] for v in influence_tuples])
    print mean_fr_influence, mean_nonfr_influence
