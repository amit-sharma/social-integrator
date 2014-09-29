from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
import numpy as np

class LocalityAnalyzer(BasicNetworkAnalyzer):
    def __init__(self, netdata):
        super(LocalityAnalyzer, self).__init__(netdata)

    def compare_circle_item_coverages(self, interact_type, minimum_friends, maximum_friends):
        circle_coverages = []
        for node in self.netdata.get_nodes_list(should_have_friends=True, should_have_interactions=True):
            coverage = node.compute_items_coverage(self.netdata.get_friends_nodes(node), interact_type, minimum_friends, maximum_friends)
            if coverage is not None:
                circle_coverages.append(coverage)
        return circle_coverages
   
    # remember, even for lastfm, if someone is mentioned as a friend, then he is
    # guaranteed to have interactions
    def compare_items_edge_coverage(self, interact_type, minimum_interactions):
        #+1 because we start node_index at 1, not 0 like numpya
        # these are items adopted by ego node because of influence from friends
        items_edge_coverage = np.zeros(self.netdata.get_max_item_id()+1, 
                                       dtype=np.dtype(np.uint32))
        # these are all items adopted by ego nodes, indexed by item_id as above
        items_total_popularity = np.zeros(self.netdata.get_max_item_id()+1, dtype=np.dtype(np.uint32))
        items_num_influencers = np.zeros(10000, dtype=np.dtype(np.uint32)) # assuming max nu. of friends=1000
        print self.netdata.get_total_num_items()+1, self.netdata.get_max_item_id()+1
        for node in self.netdata.get_nodes_list(should_have_friends=True, should_have_interactions=True):
            node.update_items_edge_coverage(self.netdata.get_friends_nodes(node), interact_type, items_edge_coverage, items_num_influencers, time_sensitive=True)
            node.update_items_popularity(interact_type, items_total_popularity)
            #print items_total_popularity[:100]
            #print items_edge_coverage[:100]

        ratio_arr = []
        filtered_pop = []
        filtered_coverage = []
        for i in range(len(items_edge_coverage)):
            if items_total_popularity[i] >= minimum_interactions:
                ratio = (items_edge_coverage[i]) / float(items_total_popularity[i]) # because each edge is counted twice!
                ratio_arr.append(ratio)
                filtered_coverage.append(items_edge_coverage[i])
                filtered_pop.append(items_total_popularity[i])
        
        degree_distr = []
        for i in xrange(len(items_num_influencers)-1, -1, -1):
            if items_num_influencers[i] > 0:
                degree_distr = items_num_influencers[0:i+1]
                break
        return filtered_coverage,filtered_pop, ratio_arr, degree_distr



