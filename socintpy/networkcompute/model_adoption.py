import heapq
from collections import defaultdict
import socintpy.networkdata.generate_fake_data as gen_fake
import socintpy.util.adoption_timeseries as adopt_time

def process_interaction(node, item_id, timestamp, rating, friends_lastm_heaps,
        kbest_lastm_heaps, rnonfr_lastm_heaps, incoming_friends,
        incoming_kbests, items_pop):

    items_pop[item_id] += 1 
    friend_counter = 0
    for _,other_item_id in friends_lastm_heaps[node.uid]:
        if other_item_id == item_id:
            friend_counter += 1

    kbest_counter = 0
    for _,other_item_id in kbest_lastm_heaps[node.uid]:
        if other_item_id == item_id:
            kbest_counter += 1
    
    #print "new_inter: %d,%d " %(new_item_id,timestamp), lastm_heaps[node.uid]
    for frid in incoming_friends:
        heapq.heapreplace(friends_lastm_heaps[frid], (timestamp, item_id))
    for neigh_id in incoming_kbests:
        heapq.heapreplace(kbest_lastm_heaps[neigh_id], (timestamp, item_id))
    return (friend_counter, kbest_counter, items_pop[item_id])

def get_initial_items_pop(nodes, interact_type):
    items_pop = defaultdict(int)
    for v in nodes:
        item_ids_list = v.get_train_ids_list(interact_type)
        for item_id in item_ids_list:
            items_pop[item_id] += 1
    return items_pop

# assumes train/test already created
def generate_adoption_data(netdata,  interact_type, split_timestamp, 
        min_interactions_per_user=1, time_window=None, time_scale=ord('o')):
    print "ALERT: Generating adoption data chronologically"
    core_nodes2 = netdata.get_nodes_list(should_have_interactions=True)
    # caution: should do it for all users with interactions to be safe
    interactions_stream, eligible_nodes  = adopt_time.get_interactions_stream(core_nodes2,
            interact_type, split_timestamp, min_interactions_per_user, after=True)
    print "Number of interactions %d after time %d" %(len(interactions_stream),
            split_timestamp)
    counter = 0
    kbest_lastm_heaps = {} # Heap storing last M interactions by neighbors
    friends_lastm_heaps = {}
    rnonfr_lastm_heaps = {}
    # care only about friends of eligible_nodes
    incoming_friends_dict = adopt_time.compute_incoming_friends(eligible_nodes)
    # consider only k-nearest neighbors of eligible nodes
    incoming_kbest_dict = adopt_time.compute_globalk_neighbors3(netdata, eligible_nodes, 
            interact_type, k=10, min_interactions_per_user=min_interactions_per_user)
    
    # consider building lastm_heap only for an eligible nodes
    friends_lastm_heaps = adopt_time.compute_initial_lastm_heaps(eligible_nodes,
            incoming_friends_dict, interact_type, split_timestamp, min_interactions_per_user,
            time_window)
    kbest_lastm_heaps = adopt_time.compute_initial_lastm_heaps(eligible_nodes,
            incoming_kbest_dict, interact_type, split_timestamp, min_interactions_per_user,
            time_window)


        
    items_pop = get_initial_items_pop(eligible_nodes, interact_type)
    # we need friends_dict for only those that appear in the test set
    # and more importantly, in the interaction stream (i.e. >=min_interactions_per_user), 
    # since that is what is compared in the suscept test.
    outf = open("adoptions.dat", "w")
    for node, item_id, timestamp, rating in interactions_stream:
        fr_count,kbest_count, itempop = process_interaction(node, item_id, timestamp, rating, friends_lastm_heaps,
                kbest_lastm_heaps, rnonfr_lastm_heaps, incoming_friends_dict[node.uid],
                incoming_kbest_dict[node.uid], items_pop)
        if node.has_friends():
            outf.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\n".format(node.uid, item_id, timestamp, rating, fr_count, kbest_count, itempop))
    outf.close()
        #print node.uid, new_item_id, timestamp
