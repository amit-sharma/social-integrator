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

    def compare_items_edge_coverage(self, interact_type, minimum_interactions):
        items_edge_coverage = np.zeros(self.netdata.get_total_num_items()+1, dtype=np.dtype(np.uint32))
        items_total_popularity = np.zeros(self.netdata.get_total_num_items()+1, dtype=np.dtype(np.uint32))
        for node in self.netdata.get_nodes_list(should_have_friends=True, should_have_interactions=True):
            node.update_items_edge_coverage(self.netdata.get_friends_nodes(node), interact_type, items_edge_coverage)
            node.update_items_popularity(interact_type, items_total_popularity)
            #print items_total_popularity[:100]
            #print items_edge_coverage[:100]

        ratio_arr = []
        filtered_pop = []
        filtered_coverage = []
        for i in range(len(items_edge_coverage)):
            if items_total_popularity[i] >= minimum_interactions:
                ratio = (items_edge_coverage[i] / 2.0) / (items_total_popularity[i]) # because each edge is counted twice!
                ratio_arr.append(ratio)
                filtered_coverage.append(items_edge_coverage[i]/2)
                filtered_pop.append(items_total_popularity[i])

        return filtered_coverage,filtered_pop, ratio_arr



