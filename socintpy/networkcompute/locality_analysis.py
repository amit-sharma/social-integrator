from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
import numpy as np
import random
import heapq
import multiprocessing as mp

def compute_influence_randomselect_parallel(netdata, nodes_list, interact_type, 
                                            control_divider, min_interactions_per_user, 
                                            time_diff, max_tries, max_node_computes, q):   
    # Find similarity on training set
    triplet_nodes = []
    counter = 0
    failed_counter = 0
    randomized_node_ids = random.sample(xrange(1, netdata.get_total_num_nodes()+1), max_tries)
    data_type="compare_train"
    data_type_code=ord(data_type[0]) 
   
    for node in nodes_list:
        if not node.has_interactions(interact_type) or not node.has_friends():
            #print "Node has no interactions. Skipping!"
            continue
        num_node_interacts = node.get_num_interactions(interact_type) # return all interactions, no check for duplicates
        fnodes = netdata.get_friends_nodes(node)
        friend_ids = node.get_friend_ids()
        for fobj in fnodes:
            if fobj.has_interactions(interact_type):
                fsim = node.compute_node_similarity(fobj, interact_type, data_type_code, 
                                            min_interactions_per_user, time_diff)
#if fsim is None:
#                        print "Error:fsim cannot be None"
                #print fsim
                num_fobj_interacts = fobj.get_num_interactions(interact_type)
                found = False
                if fsim is not None and fsim!=-1:
                    tries=0
                    r_index = 0
                    while not found and r_index < max_tries:
                        rand_node_id = randomized_node_ids[r_index]
                        rand_node = netdata.nodes[rand_node_id]
                        r_index += 1
                        if rand_node.uid in friend_ids or rand_node.uid==node.uid:
                            continue
                        rsim = node.compute_node_similarity(rand_node, interact_type, 
                                            data_type_code, min_interactions_per_user, 
                                            time_diff)
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
                if not found:
                    #print "Could not get random non-friend with sim", fsim, "in %d tries" %tries
                    failed_counter += 1
        if counter %1000==0:
            print "Done counter", counter
        if counter > max_node_computes:
            break
        counter += 1
    print "Number of failed nodes (cant find rnode)", failed_counter


    # Now compare influencer effect on test set
    data_type="influence_effect"
    data_type_code=ord(data_type[0]) 
    influence_arr = compare_influencer_effect(triplet_nodes, interact_type, 
                                              min_interactions_per_user, 
                                              time_diff, data_type_code)
    q.put(influence_arr)
    return

def compute_influence_zerosim_randomselect_parallel(netdata, nodes_list, interact_type, 
                                            control_divider, min_interactions_per_user, 
                                            time_diff, max_tries, max_node_computes, q):   
    # Find similarity on training set
    triplet_nodes = []
    counter = 0
    failed_counter = 0
    randomized_node_ids = random.sample(xrange(1, netdata.get_total_num_nodes()+1), max_tries)
    data_type="compare_train"
    data_type_code=ord(data_type[0]) 
    
    for node in nodes_list:
        if not node.has_interactions(interact_type) or not node.has_friends():
            #print "Node has no interactions. Skipping!"
            continue
        # CAUTION!! These are total interactions, not considering train/test split
        num_node_interacts = node.get_num_interactions(interact_type) # return all interactions, no check for duplicates
        fnodes = netdata.get_friends_nodes(node)
        friend_ids = node.get_friend_ids()
        for fobj in fnodes:
            if fobj.has_interactions(interact_type):
                fsim = node.compute_node_similarity(fobj, interact_type, 
                                                    data_type_code,
                                                    min_interactions_per_user,  time_diff)
#if fsim is None:
#                        print "Error:fsim cannot be None"
                #print fsim
                num_fobj_interacts = fobj.get_num_interactions(interact_type)
                found = False
                if fsim is not None and fsim!=-1 and fsim < 0.01:
                    #print num_fobj_interacts, num_node_interacts
                    tries=0
                    while not found and tries < max_tries:
                        r_index = random.randint(0,max_tries-1)
                        rand_node_id = randomized_node_ids[r_index]
                        rand_node = netdata.nodes[rand_node_id]
                        if rand_node.uid in friend_ids or rand_node.uid==node.uid:
                            continue
                        rsim = node.compute_node_similarity(rand_node, interact_type,
                                                    data_type_code, 
                                                    min_interactions_per_user, time_diff)
                        num_rnode_interacts = rand_node.get_num_interactions(interact_type)
                        if rsim is not None and rsim!=-1 and rsim >= fsim:
                            found = True
                            triplet_nodes.append((node,fobj, rand_node,
                                                  num_node_interacts,
                                                  num_fobj_interacts,
                                                  num_rnode_interacts,
                                                  fsim, rsim))
                        tries += 1
                    if not found:
                        #print "Could not get random non-friend with sim", fsim, "in %d tries" %tries
                        failed_counter += 1
        if counter %1000==0:
            print "Done counter", counter
        if counter > max_node_computes:
            break
        counter += 1
    print "Number of failed nodes with fsim==0 (cant find rnode)", failed_counter


    # Now compare influencer effect on test set
    data_type="influence_effect"
    data_type_code=ord(data_type[0]) 
    influence_arr = compare_influencer_effect(triplet_nodes, interact_type, 
                                              min_interactions_per_user, time_diff, 
                                              data_type_code)
    q.put(influence_arr)
    return

def compare_influencer_effect(triplet_nodes, interact_type, 
                              min_interactions_per_user, time_diff, data_type_code):
    influence_arr = []
    for node, fnode, rnode, num1, num2, num3, fsim, rsim in triplet_nodes:
        # this similarity is not symmetric
        inf1 = node.compute_node_similarity(fnode, interact_type, data_type_code, min_interactions_per_user, time_diff)
        inf11 = fnode.compute_node_similarity(node, interact_type, data_type_code, min_interactions_per_user, time_diff)
        if inf1 is not None and inf1 != -1:
            inf2 = node.compute_node_similarity(rnode, interact_type, data_type_code, min_interactions_per_user, time_diff)
            inf22 = rnode.compute_node_similarity(node, interact_type, data_type_code, min_interactions_per_user, time_diff)
            if inf2 is not None and inf2 !=-1:
                influence_arr.append((num1, num2, num3, fsim, rsim, 
                                      inf1, inf2, inf11, inf22))
    return influence_arr

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
    
    def compute_influence_randomselect(self, interact_type, data_type_code, control_divider):   
        # Find similarity on training set
        triplet_nodes = []
        counter = 0
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
            if counter > 10000:
                break
            counter += 1
        return triplet_nodes

    def compute_influence_knearestselect(self, interact_type, data_type, 
                                         control_divider, k):   
        # Find similarity on training set
        triplet_nodes = []
        counter = 0
        for node in self.netdata.get_nodes_list(should_have_friends=True, 
                                                should_have_interactions=True):
            if not node.has_interactions(interact_type) or not node.has_friends():
                #print "Node has no interactions. Skipping!"
                continue
            num_node_interacts = node.get_num_interactions(interact_type) # return all interactions, no check for duplicates
            fr_knearest_nodes = self.compute_knearest_neighbors(node, self.netdata.get_friends_nodes(node), 
                                                          interact_type, k, data_type)
            global_candidates = self.netdata.get_nonfriends_iterable(node)
            globalk_neighbors = self.compute_knearest_neighbors(node, global_candidates, 
                                                                interact_type, k, data_type)
            for i in range(min(len(globalk_neighbors), len(fr_knearest_nodes))):
                # These are minheaps---fails when the two heap sizes dont match
                fsim, fnode = heapq.heappop(fr_knearest_nodes)
                gsim, gnode = heapq.heappop(globalk_neighbors)
                triplet_nodes.append((node, fnode, gnode, 0, 0, 0, fsim, gsim))

            if counter %1000==0:
                print "Done counter", counter
            if counter > 10000:
                break
            counter += 1
        return triplet_nodes

    def estimate_influencer_effect(self, interact_type, split_timestamp, time_diff,
                                   control_divider=0.1, 
                                   selection_method="random", klim=None):
        cutoff_rating = self.netdata.cutoff_rating
        # Create training, test sets for all users(core and non-core)
        for node in self.netdata.get_nodes_list(should_have_interactions=True):
            node.create_training_test_sets_bytime(interact_type, split_timestamp,
                                                  cutoff_rating)
        
        data_type="compare_train"
        data_type_code=ord(data_type[0])
        if selection_method=="random":
            triplet_nodes = self.compute_influence_randomselect(interact_type, 
                                                                data_type_code,
                                                                control_divider)
        elif selection_method=="knearest":
            triplet_nodes = self.compute_influence_knearestselect(interact_type, 
                                                                data_type,
                                                                control_divider, 
                                                                klim)


        influence_arr = []
        # Compare influencer effect on test set
        data_type="influence_effect"
        data_type_code=ord(data_type[0]) 
        for node, fnode, rnode, num1, num2, num3, fsim, rsim in triplet_nodes:
            # this similarity is not symmetric
            inf1 = node.compute_node_similarity(fnode, interact_type, data_type_code, time_diff)
            inf11 = fnode.compute_node_similarity(node, interact_type, data_type_code, time_diff)
            if inf1 is not None and inf1 != -1:
                inf2 = node.compute_node_similarity(rnode, interact_type, data_type_code, time_diff)
                inf22 = rnode.compute_node_similarity(node, interact_type, data_type_code, time_diff)
                if inf2 is not None and inf2 !=-1:
                    influence_arr.append((num1, num2, num3, fsim, rsim, 
                                          inf1, inf2, inf11, inf22))
        return influence_arr

    

    def estimate_influencer_effect_parallel(self, interact_type, split_timestamp, time_diff,
                                   control_divider=0.1, min_interactions_per_user=0, 
                                   selection_method="random", klim=None, 
                                   max_tries=10000, max_node_computes=10000, num_processes=1):
        cutoff_rating = self.netdata.cutoff_rating
        if cutoff_rating is None:
            cutoff_rating = -1
        # Create training, test sets for all users(core and non-core)
        for node in self.netdata.get_nodes_list(should_have_interactions=True):
            node.create_training_test_sets_bytime(interact_type, split_timestamp,
                                                  cutoff_rating)
        
        """
        if selection_method=="random":

            triplet = self.compute_influence_randomselect_parallel(node,interact_type, 
                                                                data_type_code,
                                                                control_divider)
        elif selection_method=="knearest":
            triplet_nodes = self.compute_influence_knearestselect(interact_type, 
                                                                data_type,
                                                                control_divider, 
                                                                klim)
        """

        nodes_list = self.netdata.get_nodes_list(should_have_friends=True, should_have_interactions=True)
        partial_num_nodes = len(nodes_list) / num_processes + 1
        arg_list = []
        influence_arr_all = []
        for i in range(num_processes):
            start_index = i * partial_num_nodes
            end_index = min((i + 1) * partial_num_nodes, len(nodes_list))
            arg_list.append(nodes_list[start_index:end_index])
        
        q = mp.Queue()
        proc_list = []
        for i in range(num_processes):
            p = mp.Process(target=compute_influence_zerosim_randomselect_parallel, 
                           args=(self.netdata, arg_list[i], interact_type,
                               control_divider, min_interactions_per_user, 
                               time_diff, max_tries, max_node_computes, q))
            p.start()
            proc_list.append(p)
        
        for p in proc_list:
            curr_list = q.get() # get waits on some input by a process
            influence_arr_all.extend(curr_list)
        # join waits on end of processes
        for p in proc_list:
            p.join()
        return influence_arr_all
