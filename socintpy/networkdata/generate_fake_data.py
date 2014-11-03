import random
from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
import sys
import operator

def generate_random_preferences(netdata, interact_type, split_timestamp):
    print "ALERT: Generating some fake data..."
    for node in netdata.get_nodes_iterable(should_have_interactions=True):
        real_interactions = node.get_interactions(interact_type, 
                cutoff_rating=-1)
        interact_tuples = []
        for item_id, timestamp, rating in real_interactions:
            new_item_id = item_id
            if timestamp >= split_timestamp:
                #print "random"
                new_item_id = random.randint(1, netdata.total_num_items)
            interact_tuples.append(netdata.InteractData(new_item_id, None, timestamp, rating))
        node.remove_interactions(interact_type)
        node.store_interactions(interact_type, interact_tuples, do_sort=True)
    print"Fake data generated"

num_not_influence = 0
def select_item_dueto_influence(netdata, node, interact_type, timestamp, time_window,
        min_interactions_per_user):
    found = False
    new_item_id = None
    fids=[]
    tries = None
    global num_not_influence
    fr_nodes = netdata.get_friends_iterable(node)
    friend_nodes = [v for v in fr_nodes if v.length_test_ids >= min_interactions_per_user and v.length_train_ids >= min_interactions_per_user]
    prev_friend_actions = node.get_others_prev_actions(friend_nodes, len(friend_nodes), interact_type, timestamp, min_interactions_per_user, time_window, ord('o'), debug=False)
    if prev_friend_actions is not None and len(prev_friend_actions) > 0:
        new_item_id = prev_friend_actions[random.randint(0, len(prev_friend_actions)-1)]
        found = True
    """
    time_window = 1000000
    fids = node.get_friend_ids()
    tries = 0
    if len(fids) > 0:
        tries = 0
        while not found and tries < len(fids)*5:
            rand_fid = random.randint(1, len(fids)-1)
            rand_friend = netdata.nodes[rand_fid]

            interacts = rand_friend.get_interactions_between(interact_type, 
                    start_timestamp=timestamp-time_window, end_timestamp=timestamp,
                    cutoff_rating=-1)
            if len(interacts) > 0:
                rand_interact_id = random.randint(0,len(interacts)-1)
                new_item_id = interacts[rand_interact_id][0] # first val in tuple is item_id
                found = True
            tries += 1
    """
    if not found:
        #print "WARN: Not assigned by influence", found, len(fids), tries
        new_item_id = random.randint(1, netdata.total_num_items)
        num_not_influence += 1
    return new_item_id

friends_share=0; nonfriends_share=0; num_not_homophily=0
def select_item_dueto_homophily(netdata, node, interact_type, timestamp, time_window,
        globalk_neighbors_dict, min_interactions_per_user):
    global friends_share, nonfriends_share, num_not_homophily
    found = False
    new_item_id = None
    tries = None
    globalk_neighbors = globalk_neighbors_dict[node.uid]
    nonfriend_nodes = [v for _,v in globalk_neighbors if v.length_test_ids >= min_interactions_per_user and v.length_train_ids >= min_interactions_per_user]
    prev_friend_actions = node.get_others_prev_actions(nonfriend_nodes, len(nonfriend_nodes), interact_type, timestamp, min_interactions_per_user, time_window, ord('o'), debug=False)
    if prev_friend_actions is not None and len(prev_friend_actions) > 0:
        new_item_id = prev_friend_actions[random.randint(0, len(prev_friend_actions)-1)]
        found = True
    #global_candidates = netdata.get_nonfriends_iterable(node)
    """
    data_type="compare_train"
    global_candidates = netdata.get_othernodes_iterable(node, should_have_interactions=True)
    globalk_neighbors = BasicNetworkAnalyzer.compute_knearest_neighbors(node, global_candidates, 
                                                        interact_type, k, data_type, 
                                                        cutoff_rating = -1,
                                                        min_interactions_per_user=1,
                                                        time_diff=-1, time_scale=ord('w'))
    """
    """
    time_window = 1000000
    globalk_neighbors = globalk_neighbors_dict[node.uid]
    found = False
    new_item_id = None
    tries = 0
    while not found and tries <len(globalk_neighbors)*2:
        rand_neighbor_id = random.randint(0, len(globalk_neighbors)-1)
        rand_neighbor = globalk_neighbors[rand_neighbor_id][1]

        interacts = rand_neighbor.get_interactions_between(interact_type, 
                start_timestamp=timestamp-time_window, end_timestamp=timestamp,
                cutoff_rating=-1)
        if len(interacts) > 0:
            rand_interact_id = random.randint(0,len(interacts)-1)
            new_item_id = interacts[rand_interact_id][0] # first val in tuple is item_id
            found = True
            if rand_neighbor_id in node.get_friend_ids():
                friends_share += 1
            else:
                nonfriends_share += 1
        tries += 1
    """
    if not found:
        #print "WARN: Not assigned by homophily", found, len(globalk_neighbors), tries
        new_item_id = random.randint(1, netdata.total_num_items)
        num_not_homophily += 1
    return new_item_id

def select_item_dueto_random(items_pop):
    rand_index = random.randint(0, len(items_pop)-1) 
    #print len(items_pop), rand_index
    items_pop.append(items_pop[rand_index])
    return items_pop[rand_index]


def get_interactions_stream(nodes_iterable, interact_type, split_timestamp,
        min_interactions_per_user):
    interactions_stream = []
    items_pop = {}
    for node in nodes_iterable: 
        if node.has_friends() and node.length_train_ids >=min_interactions_per_user and node.length_test_ids>=min_interactions_per_user:
            node_interacts = node.get_interactions_after(interact_type,
                    split_timestamp=split_timestamp, cutoff_rating=-1)
            new_arr = [(node, item_id, timestamp, rating) for (
                    item_id, timestamp, rating) in node_interacts] 
            interactions_stream.extend(new_arr)
    #print [(node, item_id, timestamp, rating) for node, item_id, timestamp, rating in interactions_stream[1:10]]
    interactions_stream = sorted(interactions_stream, key=operator.itemgetter(2))
    return interactions_stream

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

def get_items_dup_array(netdata, interact_type):
    items_all = []
    for v in netdata.get_nodes_iterable(should_have_interactions=True):
        item_ids_list = v.get_train_ids_list(interact_type)
        for item_id in item_ids_list:
            items_all.append(item_id)
    return items_all

def get_items_dup_array2(nodes, interact_type):
    items_all = []
    for v in nodes:
        item_ids_list = v.get_train_ids_list(interact_type)
        for item_id in item_ids_list:
            items_all.append(item_id)
    return items_all

# three options: random, influence or homophily
# Caution: This function, under method homophily, stores the k-best neighbors as a 
#           property of netdata!! Beware, do not change split_timestamp, 
#            interact_type, min_interactions etc.  or method after that.            
def generate_fake_preferences(netdata, interact_type, split_timestamp, 
        min_interactions_per_user=1, time_window=None, time_scale=ord('o'), method="random"):
    global num_not_influence, num_not_homophily
    global friends_share, nonfriends_share
    num_not_homophily = 0
    num_not_influence = 0
    print "ALERT: Generating some fake data..."
    core_nodes = netdata.get_nodes_iterable(should_have_interactions=True,
            should_have_friends=True)
    interactions_stream  = get_interactions_stream(core_nodes, interact_type, split_timestamp, min_interactions_per_user)
    print "Number of interactions to change", len(interactions_stream)
    counter = 0
    if method=="homophily":
        all_future_nodes = [v[0] for v in interactions_stream]
        globalk_neighbors_dict = compute_globalk_neighbors(netdata, all_future_nodes, 
                interact_type, k=10, min_interactions_per_user=min_interactions_per_user)
        print "Generated k-best neighbors for all"
    elif method=="random":
        items_pop = get_items_dup_array(netdata, interact_type)
        print "Generated items popularity dict"

    for node, item_id, timestamp, rating in interactions_stream:
        if method == "random":
            new_item_id = select_item_dueto_random(items_pop)#.randint(1, netdata.total_num_items)
        elif method == "influence":
            new_item_id = select_item_dueto_influence(netdata, node, interact_type,
                    timestamp, time_window, min_interactions_per_user)
        elif method == "homophily":
            new_item_id = select_item_dueto_homophily(netdata, node, interact_type,
                    timestamp, time_window, globalk_neighbors_dict, 
                    min_interactions_per_user)
        else:
            print "Invalid fake prefs method"; sys.exit(1)
        ret = node.change_interacted_item(interact_type, item_id, new_item_id, timestamp)
        if ret == -1: 
            print node.get_interactions(interact_type)
            print "Cannot find in the above array", item_id
            print "Big Error: Item_id not found in interactions list."; sys.exit(1)
        counter += 1
        if counter % 10000 == 0:
            print "Done faked", counter
    if method=="influence":
        print "In influence: number of interactions that were not generated by influence:", num_not_influence
    elif method=="homophily":
        print "In homophily: friends share, and nonfriends share of fake data:", friends_share, nonfriends_share
        print "In homophily: number of interactions that were not generated by influence:", num_not_homophily
    print "Fake data generated"

    
# three options: random, influence or homophily
def generate_fake_preferences_simple(netdata, interact_type, split_timestamp, 
        time_window=None, method="random"):
    print "ALERT: Generating some fake data..."
    interactions_stream  = get_interactions_stream(netdata.get_nodes_iterable(should_have_interactions=True, should_have_friends=True), interact_type, split_timestamp)
    print "Number of interactions to change", len(interactions_stream)
    counter = 0
    if method=="homophily":
        all_future_nodes = [v[0] for v in interactions_stream]
        globalk_neighbors_dict = compute_globalk_neighbors(netdata, all_future_nodes, 
                interact_type, k=10)
        items_pop_dict = {}
        for uid_key, neighbors in globalk_neighbors_dict:
            items_pop_dict[uid_key]  = get_items_dup_array2(neighbors, interact_type)
        print "Generated k-best neighbors for all"
    elif method=="random":
        items_pop = get_items_dup_array(netdata, interact_type)
        print "Generated items popularity dict"
    elif method=="influence":
        items_pop_dict = {}
        for node in netdata.get_nodes_iterable(should_have_interactions=True, should_have_friends=True):
            items_pop_dict[node.uid] = get_items_dup_array2(netdata.get_friends_iterable(node), interact_type)
        print "Generated friends items dict"

    for node, item_id, timestamp, rating in interactions_stream:
        new_item_id = item_id
        if method == "random":
            new_item_id = select_item_dueto_random(items_pop)#.randint(1, netdata.total_num_items)
        elif method == "influence":
            if node.uid in items_pop_dict:
                new_item_id = select_item_dueto_random(items_pop_dict[node])
        elif method == "homophily":
            # use k=10 or some klim
            new_item_id = select_item_dueto_random(items_pop_dict[node])
        else:
            print "Invalid fake prefs method"; sys.exit(1)
        if new_item_id != item_id:
            ret = node.change_interacted_item(interact_type, item_id, new_item_id, timestamp)
        if ret == -1: 
            print node.get_interactions(interact_type)
            print "Cannot find in the above array", item_id
            print "Big Error: Item_id not found in interactions list."; sys.exit(1)
        counter += 1
        if counter % 1000000 == 0:
            print "Done faked", counter
        
    print "Fake data generated"
