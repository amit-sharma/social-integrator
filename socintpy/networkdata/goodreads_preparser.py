#import xml.etree.ElementTree as etree
from lxml import etree
from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
import socintpy.util.utils as utils
from collections import namedtuple
#from datetime import datetime

class GoodreadsDataPreparser(NetworkDataPreparser):
    interact_types_dict = {'rate': 0}
    interaction_types = range(1)
    RATE_ACTION = interaction_types[0]
    NodeData = namedtuple('NodeData', 'original_node_id interaction_types')
    ItemData = namedtuple('ItemData', 'item_id original_item_id average_rating popularity')
    InteractData = namedtuple('InteractData', 'item_id timestamp rating')
    EdgeData = namedtuple('EdgeData', 'receiver_id')

    def __init__(self, data_path, node_impl, cutoff_rating):
        NetworkDataPreparser.__init__(self, node_impl)
        self.datadir = data_path
        self.nodes_filename = data_path + "goodreads.300k.users.xml"
        self.items_filename = data_path + "goodreads.300k.items.xml"
        self.edges_filename = data_path + "goodreads.300k.edges.newxml"
        self.interactions_filename = data_path + "goodreads.300k.collections.txt"
        #self.interaction_types = range(1)
        #rate = range(1)
        #self.nodes = [None]*300000 # so that we can index it for smaller datasetsa
        self.node_counter = 0
        self.node_id_map = {}
        self.cutoff_rating = cutoff_rating
        self.copy_timestamps = []

    def get_all_data(self):
        self.read_nodes_file()
        print self.nodes[1]
        self.read_items_file()
        self.read_edges_file()
        self.read_interactions_file()
        del self.node_id_map

    def read_nodes_file(self):
        context = etree.iterparse(self.nodes_filename, events=('end',), tag="user")
        nodes_iter = utils.fast_iter(context, self.handle_nodes)

        self.nodes.insert(self.node_counter, None)
        self.node_counter += 1 # to offset for a 1-based node id and zero-based list index
        for uid, node_data in nodes_iter:
            #assert self.node_counter == uid
            nnode = self.create_network_node(self.node_counter, should_have_friends=True, should_have_interactions=True, node_data=node_data)
            self.nodes.insert(self.node_counter, nnode)
            self.node_id_map[uid] = self.node_counter
            self.node_counter += 1
        print "All core nodes stored", len(self.nodes)-1
        return self.nodes


    def read_items_file(self):
        context = etree.iterparse(self.items_filename, events=('end',), tag="item")
        items_iter = utils.fast_iter(context, self.handle_items)

        self.items.insert(0, None)
        for itemid, itemdata in items_iter:
            self.items.insert(itemid, itemdata)
        print "all items stored", len(self.items)
        return self.items

    def read_interactions_file(self):
        #self.interactions_dict = {}
        inter_file = open(self.interactions_filename)
        counter = 0
        #Interact_data = namedtuple('idata', 'user_id item_id timestamp rating')
        num_interacts = 0
        ilist = []
        prev_user = None
        for line in inter_file:
            cols = line.strip(" \n\t").split(",")
            user_id = int(cols[0])
            item_id = int(cols[1])
            #timestamp = datetime.strptime(cols[2], "%m/%d/%Y %I:%M:%S %p")
            timestamp = cols[2]
            if cols[3] != 'NaN' and int(cols[3]) >= self.cutoff_rating: # mimicking as if the invalid lines are not there in the file, so not reading them at all
                rating = int(cols[3])
                new_interaction = GoodreadsDataPreparser.InteractData(item_id, timestamp, rating)
                self.copy_timestamps.append(timestamp)
                if prev_user is not None and prev_user != user_id:
                    if prev_user in self.node_id_map:
                        self.nodes[self.node_id_map[prev_user]].store_interactions(self.RATE_ACTION, ilist)
                        counter += len(ilist)
                    num_interacts = 0
                    ilist = []
                ilist.append(new_interaction)
                num_interacts += 1

                prev_user = user_id
            
                #if counter > 1000:
                #    break
        if prev_user in self.node_id_map:
            self.nodes[self.node_id_map[prev_user]].store_interactions(self.RATE_ACTION, ilist)
        print "Relevant interactions stored", counter

    @staticmethod
    def handle_nodes(elem):
        original_uid = elem.get("original_user_id")
        uid = int(elem.get("user_id"))
        node_data = GoodreadsDataPreparser.NodeData(original_uid, GoodreadsDataPreparser.interaction_types)
        #newnode = NetworkNode(uid, should_have_friends=True, should_have_interactions=True,
        #                      node_data={'interactions': GoodreadsDataPreparser.interaction_types})
        return (uid, node_data)

    @staticmethod
    def handle_items(elem):
        item_id = int(elem.get("item_id"))
        item_data = GoodreadsDataPreparser.ItemData(item_id, elem.get("original_item_id"), elem.get('average_rating'),
                                                    elem.get('popularity'))
        return (item_id, item_data)

    @staticmethod
    def handle_edges(elem):
        sender_id = int(elem.get("sender_id"))
        receiver_id = int(elem.get("receiver_id"))
        edge_data = None
        #if sender_id < receiver_id:
        #    key = sender_id + receiver_id
        #else:
        #    key = receiver_id + sender_id
        return (sender_id, receiver_id), edge_data


    def read_edges_file(self):
        context = etree.iterparse(self.edges_filename, events=('end',), tag="edge")
        edges_iter = utils.fast_iter(context, self.handle_edges)
        count_dict = {}
        flist = []
        prev_sender = None
        curr_sender = None
        new_friends = []
        node_data = GoodreadsDataPreparser.NodeData(None, GoodreadsDataPreparser.interaction_types)
        counter = 0
        max_core_nodes_index = self.node_counter-1 #assuming only core nodes were fetched before this
        for (sender_id, receiver_id), _ in edges_iter:
            curr_sender = sender_id
            # if this is a core user, makes sure that only edges of core users are read
            if  sender_id in self.node_id_map and self.node_id_map[sender_id] <=max_core_nodes_index:
                if receiver_id not in self.node_id_map:
                    nnode = self.create_network_node(self.node_counter, should_have_friends=False, should_have_interactions=True, node_data=node_data)
                    new_friends.append((self.node_counter, nnode))
                    self.node_id_map[receiver_id] = self.node_counter
                    self.node_counter += 1

                if prev_sender is not None and curr_sender != prev_sender:
                    self.nodes[self.node_id_map[prev_sender]].store_friends(flist)
                    flist = []
                new_edge = GoodreadsDataPreparser.EdgeData(self.node_id_map[receiver_id])
                flist.append(new_edge)
                prev_sender = curr_sender
                counter += 1
        if prev_sender in self.node_id_map and self.node_id_map[prev_sender] <=max_core_nodes_index:
            self.nodes[self.node_id_map[prev_sender]].store_friends(flist)
        
        # assigning unique incremented index to each new node
        for uid, node_obj in new_friends:
            self.nodes.insert(uid, node_obj)
        print "All edges stored", counter



if __name__ == "__main__":
    x = GoodreadsDataPreparser(
        node_filename='/home/asharma/datasets/goodreads/goodreads.300k.users.xml',
        item_filename='/home/asharma/datasets/goodreads/goodreads.300k.items.xml',
        interaction_filename='/home/asharma/datasets/goodreads/goodreads.300k.collections.txt',
        edge_filename="/home/asharma/datasets/goodreads/goodreads.300k.edges.xml"
    )
    u_dict = x.read_nodes_file()
    print u_dict["1"]


