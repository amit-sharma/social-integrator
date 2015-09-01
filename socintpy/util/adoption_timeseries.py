import socintpy.cythoncode.cnetwork_node as cycode
import operator
import heapq
from collections import defaultdict

def get_interactions_stream(nodes_iterable, interact_type, split_timestamp,
        min_interactions_per_user, after=True):
    interactions_stream = []
    eligible_nodes = []
    items_pop = {}
    for node in nodes_iterable: 
        # check this so that friends etc. are compatible with the test we do later
        # it is as if other nodes do not exist
        if node.length_train_ids >=min_interactions_per_user and node.length_test_ids>=min_interactions_per_user:
            if after:
                node_interacts = node.get_interactions_after(interact_type,
                        split_timestamp=split_timestamp, cutoff_rating=-1)
            else:
                node_interacts = node.get_interactions_before(interact_type,
                        split_timestamp=split_timestamp, cutoff_rating=-1)
            new_arr = [(node, item_id, timestamp, rating) for (
                    item_id, timestamp, rating) in node_interacts] 
            interactions_stream.extend(new_arr)
            eligible_nodes.append(node)
    #print [(node, item_id, timestamp, rating) for node, item_id, timestamp, rating in interactions_stream[1:10]]
    interactions_stream = sorted(interactions_stream, key=operator.itemgetter(2))#, reverse=not after)
    return interactions_stream, eligible_nodes

def update_lastm_heaps(node, item_id, timestamp, rating, incoming_neighs,lastm_heaps):
    for neigh_id in incoming_neighs:
        heapq.heapreplace(lastm_heaps[neigh_id], (timestamp, item_id))
    return

#TODO Think carefully about what to sample nodes by
def compute_initial_lastm_heaps(nodes, incoming_neighs_dict, interact_type, split_timestamp,
        min_interactions_per_user, time_window):
    lastm_heaps = {}
    items_pop = defaultdict(int)
    for node in nodes:
        lastm_heaps[node.uid] = [(0,-1)]*time_window
        heapq.heapify(lastm_heaps[node.uid])
    interactions_stream_before, _ = get_interactions_stream(nodes, interact_type,
            split_timestamp, min_interactions_per_user, after=False)
    for node, item_id, timestamp, rating in interactions_stream_before:
        update_lastm_heaps(node, item_id, timestamp, rating, incoming_neighs_dict[node.uid], lastm_heaps)
        items_pop[item_id] += 1
    return lastm_heaps#, items_pop

def compute_incoming_friends(nodes_iter):
    incoming_fr_dict = defaultdict(list)
    temp_nodes_dict = {}
    for node in nodes_iter:
        if node.uid not in temp_nodes_dict:
            if node.has_friends():
                for frid in node.get_friend_ids():
                    incoming_fr_dict[frid].append(node.uid)
            temp_nodes_dict[node.uid] = True
    return incoming_fr_dict

def compute_globalk_neighbors3(netdata, all_nodes, interact_type, k, min_interactions_per_user):
    if netdata.global_k_incoming is not None:
        print "Got precomputed global k-nearest neighbors!"
        return netdata.global_k_incoming
    data_type="compare_train"
    data_type_code = ord(data_type[0])
    print len(netdata.get_nodes_list(should_have_interactions=True)), len(all_nodes)
    globalk_neighbors_dict = cycode.compute_global_kbest_neighbors_wrapper(all_nodes,
            interact_type,
            data_type_code, k)

    incoming_neigh_dict = defaultdict(list)
    temp_nodes_dict = {}
    for node_id, neighs in globalk_neighbors_dict.iteritems():
        count = 0
        #if not visited[node.uid]: continue
        frids = netdata.get_node_from_id(node_id).get_friend_ids()
        for neigh_id in neighs:
            incoming_neigh_dict[neigh_id].append(node_id)
            if neigh_id in frids:
                count += 1
        if node_id %100 ==0:
            print count, len(frids), len(neighs), count/len(neighs)

    netdata.global_k_incoming = incoming_neigh_dict
    return incoming_neigh_dict
"""
def compute_globalk_neighbors2(netdata, all_nodes, interact_type, k, min_interactions_per_user):
    if netdata.global_k_incoming is not None:
        print "Got precomputed global k-nearest neighbors!"
        return netdata.global_k_incoming
    globalk_incoming_dict = defaultdict(list)
    globalk_neighbors_dict = {}
    data_type="compare_train"
    counter = 0
    counter_nodes = 0
    #print "Number of nodes for which need to compute global_knearest", len(all_nodes)
    for node in all_nodes:
        counter += 1
        if node.uid not in globalk_neighbors_dict:
            global_candidates = netdata.get_othernodes_iterable(node, should_have_interactions=True)
            globalk_neighbors = BasicNetworkAnalyzer.compute_knearest_neighbors(node, global_candidates, 
                                                                interact_type, k, data_type=data_type, 
                                                                cutoff_rating = -1,
                                                                min_interactions_per_user=min_interactions_per_user,
                                                                time_diff=-1, time_scale=ord('w'))
            if len(globalk_neighbors) == 0:
                print node.uid, node.get_num_interactions(interact_type), node.length_train_ids,  k
            for sim,neighnode in globalk_neighbors:
                globalk_incoming_dict[neighnode.uid].append(node.uid)
            globalk_neighbors_dict[node.uid]= True
            counter_nodes += 1
            if counter_nodes % 100 == 0:
                print "Done for computing k-similar global neighbors for node", counter_nodes, " at interaction ", counter
    netdata.global_k_incoming = globalk_incoming_dict
    return globalk_incoming_dict

# warning: assumes that train data has been made beforehand
def compute_globalk_neighbors(netdata, all_nodes, interact_type, k, min_interactions_per_user):
    if netdata.global_k_neighbors is not None:
        print "Got precomputed global k-nearest neighbors!"
        return netdata.global_k_neighbors
    globalk_neighbors_dict = {}
    data_type="compare_train"
    counter = 0
    counter_nodes = 0
    #print "Number of nodes for which need to compute global_knearest", len(all_nodes)
    for node in all_nodes:
        counter += 1
        if node.uid not in globalk_neighbors_dict:
            global_candidates = netdata.get_othernodes_iterable(node, should_have_interactions=True)
            globalk_neighbors = BasicNetworkAnalyzer.compute_knearest_neighbors(node, global_candidates, 
                                                                interact_type, k, data_type=data_type, 
                                                                cutoff_rating = -1,
                                                                min_interactions_per_user=min_interactions_per_user,
                                                                time_diff=-1, time_scale=ord('w'))
            if len(globalk_neighbors) == 0:
                print node.uid, node.get_num_interactions(interact_type), node.length_train_ids,  k
            globalk_neighbors_dict[node.uid] = globalk_neighbors
            counter_nodes += 1
            if counter_nodes % 100 == 0:
                print "Done for computing k-similar global neighbors for node", counter_nodes, " at interaction ", counter
    netdata.global_k_neighbors = globalk_neighbors_dict
    return globalk_neighbors_dict
"""
