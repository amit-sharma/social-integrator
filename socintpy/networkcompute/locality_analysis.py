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
            node.update_items_edge_coverage(self.netdata.get_friends_nodes(node), interact_type, items_edge_coverage, items_num_influencers, time_sensitive=True, time_diff=86400) # time_diff in seconds
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

    def estimate_influencer_effect(self, interact_type, split_timestamp, cutoff_rating=-1, control_divider=0.1):
        # Create training, test sets for all users(core and non-core)
        for node in self.netdata.get_nodes_list(should_have_interactions=True):
            node.create_training_test_sets_bytime(interact_type, split_timestamp,
                                                  cutoff_rating)
        
        triplet_nodes = []
        counter = 0
        data_type="compare_train"
        data_type_code=ord(data_type[0]) 
        # Find similarity on training set
        for node in self.netdata.get_nodes_list(should_have_friends=True, 
                                                should_have_interactions=True):
            if not node.has_interactions(interact_type) or not node.has_friends():
                #print "Node has no interactions. Skipping!"
                continue
            num_node_interacts = node.get_num_interactions(interact_type) # return all interactions, no check for duplicates
            fnodes = self.netdata.get_friends_nodes(node)
            friend_ids = node.get_friend_ids()
            for fobj in fnodes:
                if fobj.has_interactions(interact_type):
                    fsim = node.compute_node_similarity(fobj, interact_type, data_type_code)
#if fsim is None:
#                        print "Error:fsim cannot be None"
                    #print fsim
                    num_fobj_interacts = fobj.get_num_interactions(interact_type)
                    found = False
                    if fsim is not None and fsim!=-1:
                        tries=0
                        while not found:
                            rand_node = self.netdata.get_random_node()
                            if rand_node.uid in friend_ids or rand_node.uid==node.uid:
                                continue
                            rsim = node.compute_node_similarity(rand_node, interact_type, data_type_code)
                            num_rnode_interacts = rand_node.get_num_interactions(interact_type)
                            if rsim is not None and rsim!=-1:
                                if round(fsim/control_divider)==round(rsim/control_divider):
                                    found = True
                                    triplet_nodes.append((node,fobj, rand_node,
                                                          num_node_interacts,
                                                          num_fobj_interacts,
                                                          num_rnode_interacts,
                                                          fsim, rsim))
                            tries += 1
                            if tries > 50000:
                                print "Could not get random non-friend with sim", fsim
                                break
            if counter %1000==0:
                print "Done counter", counter
            counter += 1


        influence_arr = []
        # Compare influencer effect on test set
        data_type="influence_effect"
        data_type_code=ord(data_type[0]) 
        for node, fnoe, rnode, num1, num2, num3, fsim, rsim in triplet_nodes:
            # this similarity is not symmetric
            inf1 = node.compute_node_similarity(fnode, interact_type, data_type_code)
            inf11 = fnode.compute_node_similarity(node, interact_type, data_type_code)
            if inf1 is not None and inf1 != -1:
                inf2 = node.compute_node_similarity(rnode, interact_type, data_type_code)
                inf22 = rnode.compute_node_similarity(node, interact_type, data_type_code)
                if inf2 is not None and inf2 !=-1:
                    influence_arr.append((num1, num2, num3, fsim, rsim, 
                                          inf1, inf2, inf11, inf22))
        return influence_arr

