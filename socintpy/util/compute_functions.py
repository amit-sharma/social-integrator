import socintpy.util.utils as utils
from collections import defaultdict
from pprint import pprint

def test_influence(la, interact_type, time_diff, time_scale, split_timestamp, control_divider,
                   min_interactions_beforeaftersplit_per_user, max_tries, max_node_computes, num_threads,
                   max_interact_ratio_error,max_sim_ratio_error, 
                   min_friends_match_ratio, klim, nonfr_match,
                   method, allow_duplicates):
    
    #time_diff = 90000 #86400
    influence_tuples = la.estimate_influencer_effect_parallel(interact_type, split_timestamp, 
                                                     time_diff, time_scale,  
                                                     control_divider=control_divider,
                                                     min_interactions_beforeaftersplit_per_user=min_interactions_beforeaftersplit_per_user,
                                                     selection_method="random",
                                                     klim=klim, max_tries=max_tries,
                                                     max_node_computes=max_node_computes,
                                                     num_threads=num_threads,
                                                     max_interact_ratio_error=max_interact_ratio_error,
                                                     max_sim_ratio_error=max_sim_ratio_error,
                                                     min_friends_match_ratio=min_friends_match_ratio,
                                                     nonfr_match = nonfr_match,
                                                     method=method, 
                                                     allow_duplicates=allow_duplicates)
    node_test_set_sizes = [v[0] for v in influence_tuples]
    fr_inf_vals = [v[5] for v in influence_tuples]
    nonfr_inf_vals = [v[6] for v in influence_tuples]
    diff_inf_vals = [v[5]-v[6] for v in influence_tuples]
    # Considering only positive values 
    diff_inf_vals_positiveonly = [v[5]-v[6] if v[5]-v[6] > 0 else 0 for v in influence_tuples]
    mean_fr_influence = utils.mean_sd(fr_inf_vals)
    mean_nonfr_influence = utils.mean_sd(nonfr_inf_vals)
    mean_diff_influence = utils.mean_sd(diff_inf_vals)
    mean_diff_influence_positiveonly = utils.mean_sd(diff_inf_vals_positiveonly)

    fr_sim_vals = [v[3] for v in influence_tuples]
    nonfr_sim_vals = [v[4] for v in influence_tuples]
    diff_sim_vals = [v[3]-v[4] for v in influence_tuples]
    mean_fr_sim = utils.mean_sd(fr_sim_vals)
    mean_nonfr_sim = utils.mean_sd(nonfr_sim_vals)
    mean_diff_sim = utils.mean_sd(diff_sim_vals)

    print "\nTest Results"
    print "Mean FrSim={0}, Mean NonFrSim={1}, MeanDiff={2}".format(mean_fr_sim, mean_nonfr_sim, mean_diff_sim)
    print "MeanFrInf={0}, Mean NonFrInf={1}".format(mean_fr_influence, mean_nonfr_influence)
    print "MeanDiff={0}, MeanDiffPositiveOnly={1}".format(mean_diff_influence, mean_diff_influence_positiveonly)
    print len(fr_inf_vals), len(nonfr_inf_vals)
    return node_test_set_sizes, fr_sim_vals, nonfr_sim_vals, fr_inf_vals, nonfr_inf_vals

def compute_cutoff_date(netdata, interact_type, train_data_fraction):
    "Find the cutoff date based on percentage"
    month_hash = defaultdict(int)
    tot_interacts = 0
    for node in netdata.get_nodes_list(should_have_interactions=True):
        items = node.get_interactions(interact_type)
        for item_id, timestamp, _ in items:
            bin_id = timestamp /(86400)
            month_hash[bin_id] += 1
            tot_interacts += 1
    running_count = 0
    ret_month = None
    for month, count in iter(sorted(month_hash.items())):
        running_count += count
        if running_count >= train_data_fraction*tot_interacts:
            ret_month = month
            break
    return ret_month





