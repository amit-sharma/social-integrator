import socintpy.networkcompute.temporal_analyzer
import socintpy.networkcompute.locality_analysis
from socintpy.networkcompute.temporal_analyzer import TemporalAnalyzer
from socintpy.networkcompute.locality_analysis import LocalityAnalyzer
import socintpy.util.plotter as plotter
import matplotlib.pyplot as plt

def compare_local_global_commonlikes(ta, interact_type):
    for node in ta.netdata.get_nodes_iterable(should_have_friends==True):
        pass

def temporal(ta):
    x = ta.create_interactions_stream(interact_type)
    ta.plot_early_late_adoption(interact_type)

    num_friends = ta.count_friends_before_interaction(0)
    prev_users = []; freq=[]
    for k,v in num_friends.iteritems():
        prev_users.append(k)
        freq.append(v)
    plotter.plotLinesXY(prev_users, freq, labelx="Number of friends who have interacted before with an item", 
            labely="Number of interactions", xlim_val=(0,20))

def locality(la):
    items_cov_list, items_popularity, cov_ratio_list, degree_distr = la.compare_items_edge_coverage(1, minimum_interactions=1)
    print utils.mean_sd(items_cov_list)
    print utils.mean_sd(items_popularity)
    plotter.plotHist(sorted([val for val in cov_ratio_list]), "Ratio of Common Likes with friends to total popularity", "Frequency (Number of items)", logyscale=True, bins=20)
    #####plotter.plotHist(sorted([val for val in cov_ratio_list]), "Ratio of Edge coverage to total popularity", "Frequency", logyscale=True)
    #plotter.plotHist(sorted(items_popularity), "Item", "total popularity")
    plotter.plotCumulativePopularity(items_popularity, labelx="Item percentile", labely="Cum. percent of number of likes")

if __name__ == "__main__":
    global data
    ta = TemporalAnalyzer(data)
    interact_type = data.interact_types_dict["listen"]
    la = LocalityAnalyzer(data)



