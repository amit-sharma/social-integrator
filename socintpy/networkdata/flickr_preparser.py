#import xml.etree.ElementTree as etree
from lxml import etree
from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
import socintpy.util.utils as utils
from collections import namedtuple
#from datetime import datetime

class FlickrDataPreparser(NetworkDataPreparser):
    interact_types_dict = {'favorite': 0}
    interaction_types = range(1)
    RATE_ACTION = interaction_types[0]
    NodeData = namedtuple('NodeData', 'original_node_id interaction_types')
    ItemData = namedtuple('ItemData', 'item_id original_item_id created_time \
                           photo_owner num_views num_comments num_favorites')
    InteractData = namedtuple('InteractData', 'item_id timestamp rating')
    EdgeData = namedtuple('EdgeData', 'receiver_id timestamp')

    def __init__(self, data_path, node_impl, cutoff_rating, 
                 max_core_nodes, store_dataset, min_interactions_per_user):
        NetworkDataPreparser.__init__(self, node_impl, data_path,
                                      max_core_nodes=max_core_nodes, 
                                      store_dataset=store_dataset,
                                      min_interactions_per_user=min_interactions_per_user)
        self.datadir = data_path
        self.nodes_filename = data_path + "sorted-flickr-growth.txt"
        self.items_filename = data_path + "flickr-all-photos.txt"
        self.edges_filename = data_path + "sorted-flickr-growth.txt"
        self.interactions_filename = data_path + "sorted-flickr-all-photo-favorite-markings.txt"
        #self.interaction_types = range(1)
        #rate = range(1)
        #self.nodes = [None]*300000 # so that we can index it for smaller datasetsa
        self.node_counter = 0
        self.node_id_map = {}
        self.cutoff_rating = cutoff_rating
        self.cutoff_rating = None
        #self.copy_timestamps = []
        
        globals().update(self.named_tuple_dict)

    def get_all_data(self):
        if self.load_from_saved_dataset:
            self.load_dataset()
            del self.node_id_map
        else:
            self.read_nodes_file()
            self.read_edges_file()
            self.read_items_file()
            self.read_interactions_file()
            del self.node_id_map
            print "filtering data based on min_interactions_per_user=", self.min_interactions_per_user
            self.filter_min_interaction_nodes(self.interact_type_val, self.min_interactions_per_user)
#           proc_pool.close()
#           proc_pool.join()
            items_interacted_with_dict = self.compute_store_total_num_items()
            self.total_num_items = len(items_interacted_with_dict)

            # Not reading all_item_ids file for now.
            #self.item_ids_list = self.read_all_item_ids(self.datadir+"itemmap.tsv") 
            self.print_summary()

    def read_nodes_file(self):
        self.nodes.insert(self.node_counter, None)
        self.node_counter += 1 # to offset for a 1-based node id and zero-based list index
        prev_uid = None
        with open(self.nodes_filename) as f:
            for line in f:
                cols = line.split("\t")
                uid = int(cols[0])
                if uid!=prev_uid:
                    #print uid, prev_uid
                    original_uid = uid
                    node_data = FlickrDataPreparser.NodeData(original_uid, 
                            FlickrDataPreparser.interaction_types)
                    nnode = self.create_network_node(self.node_counter, 
                            should_have_friends=True, 
                            should_have_interactions=True, node_data=node_data)
                    self.nodes.insert(self.node_counter, nnode)
                    self.node_id_map[uid] = self.node_counter
                    self.node_counter += 1
                prev_uid = uid
            print "All core nodes stored", len(self.nodes)-1
        return self.nodes

    # Item ids are unchanged. The index in the items array is NOT the item_id.
    def read_items_file(self):
        with open(self.items_filename) as f:
            for line in f:
                cols = line.strip(" \n").split()
                item_id = int(cols[0])
                anon_timestamp = int(cols[1])
                anon_uploader = int(cols[2])
                num_views = int(cols[3]) if cols[3] != "\N" else -1
                num_comments = int(cols[4]) if cols[4] != "\N" else -1
                num_favorites = int(cols[5]) if cols[5] != "\N" else -1
                if anon_uploader in self.node_id_map:
                    photo_owner_id = self.node_id_map[anon_uploader]
                else:
                    photo_owner_id = -1
                itemdata = FlickrDataPreparser.ItemData(
                        item_id=item_id, original_item_id=item_id,
                        created_time=anon_timestamp, 
                        photo_owner=photo_owner_id,
                        num_views=num_views, num_comments=num_comments,
                        num_favorites=num_favorites)
                self.items.insert(item_id, itemdata)
        print "All items stored (based on flickr-all-photos.txt)", len(self.items)
        self.total_num_items = len(self.items)
        return self.items

    def read_edges_file(self):
        count_dict = {}
        flist = []
        prev_sender = None
        curr_sender = None
        new_friends = []
        node_data = FlickrDataPreparser.NodeData(None, FlickrDataPreparser.interaction_types)
        counter = 0
        max_core_nodes_index = self.node_counter-1 #assuming only core nodes were fetched before this
        with open(self.edges_filename) as f:
            for line in f:
                cols = line.strip(" \n\t").split()
                sender_id = int(cols[0])
                receiver_id = int(cols[1])
                anon_timestamp = cols[2]
                curr_sender = sender_id
                # if this is a core user, makes sure that only edges of core users are read
                # equivalent to filtering the file to contain only core senders           
                if  sender_id in self.node_id_map and self.node_id_map[sender_id] <=max_core_nodes_index:
                    if receiver_id not in self.node_id_map:
                        nnode = self.create_network_node(
                                self.node_counter, should_have_friends=False, 
                                should_have_interactions=True, node_data=node_data)
                        new_friends.append((self.node_counter, nnode))
                        self.node_id_map[receiver_id] = self.node_counter
                        self.node_counter += 1

                    if prev_sender is not None and curr_sender != prev_sender:
                        self.nodes[self.node_id_map[prev_sender]].store_friends(flist)
                        flist = []
                    new_edge = FlickrDataPreparser.EdgeData(
                            self.node_id_map[receiver_id], anon_timestamp)
                    flist.append(new_edge)
                    prev_sender = curr_sender
                    counter += 1
            if prev_sender in self.node_id_map and self.node_id_map[prev_sender] <=max_core_nodes_index:
                self.nodes[self.node_id_map[prev_sender]].store_friends(flist)

        
        # assigning unique incremented index to each new node
        for uid, node_obj in new_friends:
            self.nodes.insert(uid, node_obj)
        print "All edges stored(assuming directed edges)", counter
        print "Total number of nodes(including friends of core users)", len(self.nodes) -1 

    def read_interactions_file(self):
        #self.interactions_dict = {}
        inter_file = open(self.interactions_filename)
        counter = 0
        #Interact_data = namedtuple('idata', 'user_id item_id timestamp rating')
        num_interacts = 0
        ilist = []
        prev_user = None
        ratings_but_notconnected_flag = 0
        for line in inter_file:
            cols = line.strip(" \n\t").split()
            user_id = int(cols[0])
            item_id = int(cols[1])
            timestamp = int(cols[2])
            rating = 1 
            new_interaction = FlickrDataPreparser.InteractData(item_id, timestamp, rating)
            #self.copy_timestamps.append(timestamp)
            if prev_user is not None and prev_user != user_id:
                if prev_user in self.node_id_map:
                    self.nodes[self.node_id_map[prev_user]].store_interactions(self.RATE_ACTION, ilist)
                    counter += len(ilist)
                else:
                    ratings_but_notconnected_flag += 1

                num_interacts = 0
                ilist = []
            ilist.append(new_interaction)
            num_interacts += 1

            prev_user = user_id
        
            #if counter > 1000:
            #    break
        if prev_user in self.node_id_map:
            self.nodes[self.node_id_map[prev_user]].store_interactions(self.RATE_ACTION, ilist)
            counter += len(ilist)
        
        if ratings_but_notconnected_flag > 0:
            print "WARN: Some users have ratings but are not in the flickr-growth.txt file"
            print ratings_but_notconnected_flag
        print "Relevant interactions stored", counter





if __name__ == "__main__":
    """
    x = FlixsterDataPreparser(
        node_filename='/home/asharma/datasets/goodreads/goodreads.300k.users.xml',
        item_filename='/home/asharma/datasets/goodreads/goodreads.300k.items.xml',
        interaction_filename='/home/asharma/datasets/goodreads/goodreads.300k.collections.txt',
        edge_filename="/home/asharma/datasets/goodreads/goodreads.300k.edges.xml"
    )
    """
    u_dict = x.read_nodes_file()
    print u_dict["1"]


