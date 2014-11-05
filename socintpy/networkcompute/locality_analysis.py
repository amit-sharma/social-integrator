from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
import numpy as np
import random
import heapq
import multiprocessing as mp
import operator

def compute_susceptibility_randomselect_parallel(netdata, nodes_list, interact_type, 
                                            cutoff_rating, control_divider, min_interactions_per_user, 
                                            time_diff, time_scale, max_tries, max_node_computes,
                                            max_interact_ratio_error, nonfr_match, 
                                            allow_duplicates, q):   
    num_rand_attempts = 2.0
    final_influence = None
    for ii in range(int(num_rand_attempts)):
        influence_dict = compute_susceptibility_randomselect(netdata, nodes_list, interact_type, 
                                                cutoff_rating, control_divider, min_interactions_per_user, 
                                                time_diff, time_scale, max_tries, max_node_computes,
                                                max_interact_ratio_error, nonfr_match, allow_duplicates)
        if final_influence is None:
            final_influence = influence_dict
        else:
            for key, value in final_influence.iteritems():
                if value is not None and key in influence_dict:
                    final_influence[key] = map(operator.add,final_influence[key],
                            influence_dict[key])
                else:
                    final_influence[key] = None
    final_influence_arr = [tuple(val/num_rand_attempts for val in val_tuple) for (
            val_tuple) in final_influence.itervalues() if val_tuple is not None]

    q.put(final_influence_arr)
    return

def compute_susceptibility_randomselect(netdata, nodes_list, interact_type, 
                                            cutoff_rating, control_divider, min_interactions_per_user, 
                                            time_diff, time_scale, max_tries, max_node_computes,
                                            max_interact_ratio_error, nonfr_match,
                                            allow_duplicates):   
    # Find similarity on training set
    max_sim_ratio_error = 0.1
    triplet_nodes = []
    counter = 0
    failed_counter = 0
    eligible_nodes_counter = 0
    count_success = 0
    edges_counter = 0
    total_tries_counter = 0
    time_saved_counter = 0
   
    if max_tries is None:
        max_tries = netdata.get_total_num_nodes()
    randomized_node_ids = random.sample(xrange(1, netdata.get_total_num_nodes()+1), max_tries)
    
    data_type="compare_train"
    data_type_code=ord(data_type[0]) 
    #sim_dict = {}
    for node in nodes_list:
        nonfr_ids = {}
        sim_dict = {}
        num_node_interacts = node.get_num_interactions(interact_type) # return all interactions, no check for duplicates
        #if not node.has_interactions(interact_type) or not node.has_friends():
        if node.length_train_ids < min_interactions_per_user or node.length_test_ids <min_interactions_per_user or not node.has_friends():
            #print "Node has no interactions. Skipping!"
            counter +=1
            continue
        eligible_nodes_counter += 1
        fnodes = netdata.get_friends_nodes(node)
        control_nonfr_nodes = []
        avg_fsim = 0
        avg_rsim = 0
        num_eligible_friends = 0
        selected_friends = []
        friend_ids = node.get_friend_ids()
        edges_counter += len(friend_ids)
        for fobj in fnodes:
            num_fobj_interacts = fobj.get_num_interactions(interact_type)
            if fobj.length_train_ids >=min_interactions_per_user and fobj.length_test_ids >=min_interactions_per_user:
                """
                fsim2 = node.compute_node_similarity(fobj, interact_type, 
                        cutoff_rating, data_type_code, 
                        min_interactions_per_user, time_diff=500000, time_scale=ord('w'))#time_diff=-1, time_scale=time_scale)
                """
                if (fobj.uid,node.uid) in sim_dict:
                    fsim = sim_dict[(fobj.uid,node.uid)]
                elif (node.uid,fobj.uid) in sim_dict:
                    fsim = sim_dict[(node.uid,fobj.uid)]
                else:
                    fsim = node.compute_node_similarity(fobj, interact_type, 
                            cutoff_rating, data_type_code, 
                            min_interactions_per_user, time_diff=-1, time_scale=time_scale)
                    sim_dict[(fobj.uid, node.uid)] = fsim
#if fsim is None:
#                        print "Error:fsim cannot be None"
                #print fsim
                found = False
                if fsim is not None and fsim!=-1:
                    num_eligible_friends += 1
                    total_tries_counter += 1
                    tries=0
                    if nonfr_match=="random":
                        randomized_node_ids = random.sample(xrange(1, netdata.get_total_num_nodes()+1), max_tries)
                    elif nonfr_match=="kbest":
                        global_candidates = netdata.get_othernodes_iterable(fobj, should_have_interactions=True)
                        globalk_neighbors = BasicNetworkAnalyzer.compute_knearest_neighbors(fobj, global_candidates, 
                                                                interact_type,1000, data_type=data_type, 
                                                                cutoff_rating = -1,
                                                                min_interactions_per_user=min_interactions_per_user,
                                                                time_diff=-1, time_scale=ord('w'))
                        randomized_node_ids = [heapq.heappop(globalk_neighbors)[1].uid for h in xrange(len(globalk_neighbors))]
                        randomized_node_ids.reverse()
                    elif nonfr_match=="serial":
                        randomized_node_ids = range(1, max_tries+1)
                    else:
                        print "Error in parameter"; sys.exit(1)
                    r_index = 0
                   
                    while not found and r_index < max_tries and r_index<len(randomized_node_ids):
                        rand_node_id = randomized_node_ids[r_index]
                        r_index += 1
                        if rand_node_id in nonfr_ids:
                            continue
                        rand_node = netdata.nodes[rand_node_id]
                        if rand_node.length_train_ids >=min_interactions_per_user and rand_node.length_test_ids >=min_interactions_per_user:
                            ratio_train = abs(rand_node.length_train_ids-fobj.length_train_ids)/float(fobj.length_train_ids)
                            if ratio_train <= max_interact_ratio_error: 
                                if rand_node.uid not in friend_ids and rand_node.uid!=node.uid:
                                    if (rand_node.uid,node.uid) in sim_dict: 
                                        rsim = sim_dict[(rand_node.uid,node.uid)]
                                        time_saved_counter += 1
                                    elif (node.uid,rand_node.uid) in sim_dict:
                                        rsim = sim_dict[(node.uid,rand_node.uid)]
                                        time_saved_counter += 1
                                    else:
                                        rsim = node.compute_node_similarity(rand_node, interact_type, 
                                                        cutoff_rating, data_type_code, min_interactions_per_user, 
                                                        time_diff=-1, time_scale=time_scale)
                                        sim_dict[(rand_node.uid, node.uid)] = rsim
                                        """
                                        rsim2 = node.compute_node_similarity(rand_node, interact_type, 
                                                        cutoff_rating, data_type_code, min_interactions_per_user, 
                                                        time_diff=500000, time_scale=ord('w'))#time_diff=-1, time_scale=time_scale)
                                                        #time_diff=-1, time_scale=time_scale)
                                        """
                                    num_rnode_interacts = rand_node.get_num_interactions(interact_type)
                                    if rsim is not None and rsim!=-1:
                                        sim_diff = abs(rsim-fsim)
                                        if (fsim==0 and sim_diff<=0.00001) or (fsim>0 and
                                                sim_diff/fsim <= max_sim_ratio_error):# and (fsim2 >0 and abs(rsim2-fsim2)/fsim2<=max_sim_ratio_error)):
                                            """
                                            fr_nonfr_sim = fobj.compute_node_similarity(rand_node, interact_type, 
                                                        cutoff_rating, data_type_code, min_interactions_per_user, 
                                                        time_diff=-1, time_scale=time_scale)
                                            print fr_nonfr_sim, node.length_train_ids, fobj.length_train_ids, rand_node.length_train_ids, fsim, rsim, r_index, max_tries
                                            if fr_nonfr_sim > 2*fsim:
                                            """
                                            if True:
                                                found = True
                                                avg_fsim += fsim
                                                avg_rsim += rsim
                                                nonfr_ids[rand_node_id] = True
                                                control_nonfr_nodes.append(rand_node)
                                                selected_friends.append(fobj)
                        tries += 1
                    if not found:
                        #print "Could not get random non-friend with sim", fsim, "in %d tries" %tries
                        failed_counter += 1
        #print "SEE:", len(control_nonfr_nodes), num_eligible_friends
        if num_eligible_friends >0 and len(control_nonfr_nodes) >= 1*num_eligible_friends:
            avg_fsim = avg_fsim/float(len(control_nonfr_nodes))
            avg_rsim = avg_rsim/float(len(control_nonfr_nodes))
            #print num_eligible_friends, len(selected_friends)
            if len(selected_friends) != len(control_nonfr_nodes):
                print "ALERT: Something is wrong here!!"; sys.exit(2)
            if len(control_nonfr_nodes) != num_eligible_friends:
                print "WARN: Cannot match all eligible friends", num_eligible_friends, len(control_nonfr_nodes)
            #print node.uid, [fr.uid for fr in selected_friends]
            triplet_nodes.append((node, selected_friends, control_nonfr_nodes, 
                                 0, 0, 0, avg_fsim, avg_rsim))
            count_success +=1
        if counter %1000==0:
            print "Done counter", counter
        if max_node_computes is not None:
            if counter > max_node_computes:
                print counter, max_node_computes
                break
        counter += 1
    print "\n--Number of nodes assigned to me(with interactions and friends):", len(nodes_list)
    print "--Eligible nodes (with interactions > %d): " %min_interactions_per_user, eligible_nodes_counter
    print "--Total Edges from eligible nodes:", edges_counter
    #print "--Eligible friend-edges (with friend hving interactions >%d): " %min_interactions_per_user, eligible_edges_counter
    print "--Number of tries (and successful caches) to find random non-friend:", total_tries_counter, time_saved_counter
    print "--Number of  successful nodes (can find rnodes):", count_success
    print "--Successful triplets:", len(triplet_nodes) 


    # Now compare influencer effect on test set
    data_type="influence_effect"
    data_type_code=ord(data_type[0]) 
    influence_arr = compare_susceptibility_effect(triplet_nodes, interact_type, 
                                              cutoff_rating, min_interactions_per_user, 
                                              time_diff, time_scale, data_type_code,
                                              allow_duplicates)
    return influence_arr


def compute_influence_randomselect_parallel(netdata, nodes_list, interact_type, 
                                            cutoff_rating, control_divider, min_interactions_per_user, 
                                            time_diff, time_scale, max_tries, max_node_computes,
                                            max_interact_ratio_error, q):   
    num_rand_attempts = 2.0
    final_influence = None
    for ii in range(int(num_rand_attempts)):
        influence_dict = compute_influence_randomselect(netdata, nodes_list, interact_type, 
                                                cutoff_rating, control_divider, min_interactions_per_user, 
                                                time_diff, time_scale, max_tries, max_node_computes,
                                                max_interact_ratio_error)
        if final_influence is None:
            final_influence = influence_dict
        else:
            for key, value in final_influence.iteritems():
                if value is not None and key in influence_dict:
                    final_influence[key] = map(operator.add,final_influence[key],
                            influence_dict[key])
                    #print "yes"
                else:
                    final_influence[key] = None
    final_influence_arr = [tuple(val/num_rand_attempts for val in val_tuple) for (
            val_tuple) in final_influence.itervalues() if val_tuple is not None]

    #print final_influence_arr
    q.put(final_influence_arr)
    return

def compute_influence_randomselect(netdata, nodes_list, interact_type, 
                                            cutoff_rating, control_divider, min_interactions_per_user, 
                                            time_diff, time_scale, max_tries, max_node_computes,
                                            max_interact_ratio_error):   
    # Find similarity on training set
    max_sim_ratio_error = 0.1
    triplet_nodes = []
    counter = 0
    failed_counter = 0
    eligible_nodes_counter = 0
    edges_counter = 0
    eligible_edges_counter = 0
    total_tries_counter = 0
    sum_interacts_fr = 0
    sum_interacts_nonfr = 0
    randomized_node_ids = random.sample(xrange(1, netdata.get_total_num_nodes()+1), max_tries)
    data_type="compare_train"
    data_type_code=ord(data_type[0]) 
    for node in nodes_list:
        num_node_interacts = node.get_num_interactions(interact_type) # return all interactions, no check for duplicates
        #if not node.has_interactions(interact_type) or not node.has_friends():
        if node.length_train_ids < min_interactions_per_user or node.length_test_ids <min_interactions_per_user or not node.has_friends():
            #print "Node has no interactions. Skipping!"
            counter +=1
            continue
        eligible_nodes_counter += 1
        fnodes = netdata.get_friends_nodes(node)
        friend_ids = node.get_friend_ids()
        edges_counter += len(friend_ids)
        for fobj in fnodes:
            num_fobj_interacts = fobj.get_num_interactions(interact_type)
            if fobj.length_train_ids >=min_interactions_per_user and fobj.length_test_ids >=min_interactions_per_user:
                eligible_edges_counter += 1
                fsim = node.compute_node_similarity(fobj, interact_type, cutoff_rating,  data_type_code, 
                                            min_interactions_per_user, time_diff=-1, time_scale=time_scale)
#if fsim is None:
#                        print "Error:fsim cannot be None"
                #print fsim
                found = False
                if fsim is not None and fsim!=-1:
                    total_tries_counter += 1
                    tries=0
                    r_index = 0
                    while not found and r_index < max_tries:
                        rand_node_id = randomized_node_ids[r_index]
                        rand_node = netdata.nodes[rand_node_id]
                        r_index += 1
                        if rand_node.length_train_ids >=min_interactions_per_user and rand_node.length_test_ids >=min_interactions_per_user:
                            ratio_train = abs(rand_node.length_train_ids-fobj.length_train_ids)/float(fobj.length_train_ids)
                            if ratio_train <= max_interact_ratio_error: 
                                if rand_node.uid not in friend_ids and rand_node.uid!=node.uid:
                                    rsim = node.compute_node_similarity(rand_node, interact_type, cutoff_rating, 
                                                        data_type_code, min_interactions_per_user, 
                                                        time_diff=-1, time_scale=time_scale)
                                    num_rnode_interacts = rand_node.get_num_interactions(interact_type)
                                    if rsim is not None and rsim!=-1:
                                        #if round(fsim/control_divider)==round(rsim/control_divider) and (
#rsim > fsim):
                                        sim_diff = abs(rsim-fsim)
                                        if sim_diff<=0.00001 or (fsim>0 and 
                                                sim_diff/fsim <= max_sim_ratio_error):
                                            found = True
                                            sum_interacts_fr += fobj.length_train_ids
                                            sum_interacts_nonfr += rand_node.length_train_ids
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
        if max_node_computes is not None:
            if counter > max_node_computes:
                print counter, max_node_computes
                break
        counter += 1
    print "\n--Number of nodes assigned to me(with interactions and friends):", len(nodes_list)
    print "--Eligible nodes (with interactions > %d): " %min_interactions_per_user, eligible_nodes_counter
    print "--Total Edges from eligible nodes:", edges_counter
    print "--Eligible friend-edges (with friend hving interactions >%d): " %min_interactions_per_user, eligible_edges_counter
    print "--Number of tries to find random non-friend:", total_tries_counter
    print "--Fr interactions %d non-fr interactions %d" %(sum_interacts_fr, sum_interacts_nonfr)
    print "--Number of failed nodes (cant find rnode):", failed_counter
    print "--Successful triplets:", len(triplet_nodes)


    # Now compare influencer effect on test set
    data_type="influence_effect"
    data_type_code=ord(data_type[0]) 
    influence_arr = compare_influencer_effect(triplet_nodes, interact_type, 
                                              cutoff_rating, min_interactions_per_user, 
                                              time_diff, time_scale, data_type_code)
    return influence_arr

# caution: function not updated in a while, especially wrt cythoncode
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
        if max_node_computes is not None:
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

def compare_susceptibility_effect(triplet_nodes, interact_type, cutoff_rating, 
                                 min_interactions_per_user, time_diff, time_scale,
                                 data_type_code, allow_duplicates):
    """
    new_triplet_nodes = []
    for node, fnodes, rnodes, num1, num2, num3, fsim, rsim in triplet_nodes:
        for i in range(len(fnodes)):
            new_triplet_nodes.append((node, fnodes[i], rnodes[i], num1, num2, num3, fsim, rsim))
    return compare_influencer_effect(new_triplet_nodes, interact_type, cutoff_rating, 
                              min_interactions_per_user, time_diff, time_scale, data_type_code)
    """
    influence_arr = {}
    node_fnode_counter = 0
    node_rnode_counter = 0
    for node, fnodes, rnodes, num1, num2, num3, fsim, rsim in triplet_nodes:
        # this similarity is not symmetric
        inf1 = node.compute_node_susceptibility(fnodes, len(fnodes), interact_type,
                cutoff_rating, data_type_code, min_interactions_per_user, 
                time_diff, time_scale, allow_duplicates, debug=False)
        #print inf1
        if inf1 is not None and inf1 > -1:
            node_fnode_counter += 1
            inf2 = node.compute_node_susceptibility(rnodes, len(rnodes), 
                    interact_type, cutoff_rating, data_type_code, 
                    min_interactions_per_user, 
                    time_diff, time_scale, allow_duplicates, debug=False)
            #print node.uid, inf1, inf2, "\n"
            if inf2 is not None and inf2 > -1:
                node_rnode_counter += 1
                influence_arr[node.uid] = (num1, num2, num3, fsim, rsim, 
                                      inf1, inf2)

    print "--Nodes with eligible fnode influence measure:", node_fnode_counter
    print "--Nodes with eligible fnode and rnode influence measure:", node_rnode_counter
    return influence_arr

def compare_influencer_effect(triplet_nodes, interact_type, cutoff_rating, 
                              min_interactions_per_user, time_diff, time_scale, data_type_code):
    influence_arr = {}
    node_fnode_counter = 0
    node_rnode_counter = 0
    for node, fnode, rnode, num1, num2, num3, fsim, rsim in triplet_nodes:
        # this similarity is not symmetric
        inf1 = node.compute_node_similarity(fnode, interact_type, cutoff_rating,data_type_code, min_interactions_per_user, time_diff, time_scale)
        inf11 = fnode.compute_node_similarity(node, interact_type, cutoff_rating, data_type_code, min_interactions_per_user, time_diff, time_scale)
        if inf1 is not None and inf1 != -1:
            node_fnode_counter += 1
            inf2 = node.compute_node_similarity(rnode, interact_type, cutoff_rating, data_type_code, min_interactions_per_user, time_diff, time_scale)
            inf22 = rnode.compute_node_similarity(node, interact_type, cutoff_rating, data_type_code, min_interactions_per_user, time_diff,time_scale)
            if inf2 is not None and inf2 !=-1:
                node_rnode_counter += 1
                influence_arr[(node.uid, fnode.uid)]=(num1, num2, num3, fsim, rsim, 
                                      inf1, inf2, inf11, inf22)

    print "--Nodes with eligible fnode influence measure:", node_fnode_counter
    print "--Nodes with eligible fnode and rnode influence measure:", node_rnode_counter
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
    #cuation: this function has not been used/updated in a while 
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

    # caution: not used or updated
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

    #not used or updated
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

    
      #CAUTION:  assumes training test data already there
    def estimate_influencer_effect_parallel(self, interact_type, split_timestamp, time_diff,
                                   time_scale, control_divider=0.1, min_interactions_per_user=0, 
                                   selection_method="random", klim=None, 
                                   max_tries=10000, max_node_computes=10000, num_processes=1,
                                   max_interact_ratio_error=0.1,
                                   nonfr_match="random",
                                   method="influence",
                                   allow_duplicates=True):
        cutoff_rating = self.netdata.cutoff_rating
        if cutoff_rating is None:
            cutoff_rating = -1
        """
        # Create training, test sets for all users(core and non-core)
        for node in self.netdata.get_nodes_list(should_have_interactions=True):
            node.create_training_test_sets_bytime(interact_type, split_timestamp,
                                                  cutoff_rating)
        """
        
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
            if method=="influence":
                p = mp.Process(target=compute_influence_randomselect_parallel, 
                               args=(self.netdata, arg_list[i], interact_type,
                                   cutoff_rating, control_divider, 
                                   min_interactions_per_user, 
                                   time_diff, time_scale, max_tries, 
                                   max_node_computes, max_interact_ratio_error, 
                                   nonfr_match,q))
            elif method=="suscept":
                p = mp.Process(target=compute_susceptibility_randomselect_parallel, 
                               args=(self.netdata, arg_list[i], interact_type,
                                   cutoff_rating, control_divider, min_interactions_per_user, 
                                   time_diff, time_scale, max_tries,
                                   max_node_computes, max_interact_ratio_error,
                                   nonfr_match, allow_duplicates, q))
            p.start()
            proc_list.append(p)
        
        for p in proc_list:
            curr_list = q.get() # get waits on some input by a process
            influence_arr_all.extend(curr_list)
        # join waits on end of processes
        for p in proc_list:
            p.join()
        return influence_arr_all


