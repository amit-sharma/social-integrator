import socintpy.networkcompute.temporal_analyzer
from socintpy.networkcompute.temporal_analyzer import TemporalAnalyzer
import socintpy.util.plotter as plotter
import matplotlib.pyplot as plt


if __name__ == "__main__":
    ta = TemporalAnalyzer(data)
    interact_type = data.interact_types_dict["listen"]
    x = ta.create_interactions_stream(interact_type)
    ta.plot_early_late_adoption(interact_type)

    num_friends = ta.count_friends_before_interaction(0)
    prev_users = []; freq=[]
    for k,v in num_friends.iteritems():
        prev_users.append(k)
        freq.append(v)
    plotter.plotLinesXY(prev_users, freq, labelx="Number of friends who have interacted before with an item", 
            labely="Number of interactions", xlim_val=(0,20))


