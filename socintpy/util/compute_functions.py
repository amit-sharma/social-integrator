import time, datetime
import socintpy.util.utils as utils

def test_influence(la, interact_type, time_diff, split_date_str, control_divider,
                   max_tries, num_processes):
    split_timestamp = int(time.mktime(datetime.datetime.strptime(split_date_str, "%Y/%m/%d").timetuple()))
    #time_diff = 90000 #86400
    influence_tuples = la.estimate_influencer_effect_parallel(0, split_timestamp, 
                                                     time_diff, 
                                                     control_divider=control_divider, 
                                                     selection_method="random",
                                                     klim=5, max_tries=max_tries,
                                                     num_processes=num_processes)
    fr_inf_vals = [v[5] for v in influence_tuples]
    nonfr_inf_vals = [v[6] for v in influence_tuples]
    diff_inf_vals = [v[5]-v[6] for v in influence_tuples]
    mean_fr_influence = utils.mean_sd(fr_inf_vals)
    mean_nonfr_influence = utils.mean_sd(nonfr_inf_vals)
    mean_diff_influence = utils.mean_sd(diff_inf_vals)
    print mean_fr_influence, mean_nonfr_influence, mean_diff_influence
    print len(fr_inf_vals), len(nonfr_inf_vals)
    return fr_inf_vals, nonfr_inf_vals
