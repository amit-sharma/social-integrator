#import xml.etree.ElementTree as etree
from lxml import etree
from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
import socintpy.util.utils as utils
from collections import namedtuple


class GoodreadsDataPreparser(NetworkDataPreparser):
    interaction_types = range(1)
    NodeData = namedtuple('NodeData', 'original_node_id interaction_types')
    ItemData = namedtuple('ItemData', 'item_id original_item_id average_rating popularity')
    InteractData = namedtuple('InteractData', 'item_id timestamp rating')

    def __init__(self, data_path):
        NetworkDataPreparser.__init__(self)
        self.datadir = data_path
        self.nodes_filename = data_path + "goodreads.300k.users.xml"
        self.items_filename = data_path + "goodreads.300k.items.xml"
        self.edges_filename = data_path + "goodreads.300k.edges.xml"
        self.interactions_filename = data_path + "goodreads.300k.collections.txt"
        #self.interaction_types = range(1)
        #rate = range(1)


    def get_all_data(self):
        self.read_nodes_file()
        print self.nodes[1]
        self.read_items_file()
        self.read_edges_file()
        self.read_interactions_file()


    def read_nodes_file(self):
        context = etree.iterparse(self.nodes_filename, events=('end',), tag="user")
        nodes_iter = utils.fast_iter(context, self.handle_nodes)

        self.nodes.insert(0, None) # to offset for a 1-based node id and zero-based list index
        for uid, node_data in nodes_iter:
            self.nodes.insert(uid, NetworkNode(uid, has_friends=True, has_interactions=True, node_data=node_data))
        print "All nodes stored", len(self.nodes)
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
            timestamp = cols[2]
            rating = int(cols[3])  if cols[3] != 'NaN' else -1
            new_interaction = GoodreadsDataPreparser.InteractData(item_id, timestamp, rating)
            if prev_user is not None and prev_user != user_id:
                self.nodes[prev_user].store_interactions("rate", ilist)
                num_interacts = 0
                ilist = []
            ilist.append(new_interaction)
            num_interacts += 1

            prev_user = user_id

            counter += 1
            if counter > 1000:
                break
        print "All interactions stored", counter

    @staticmethod
    def handle_nodes(elem):
        original_uid = elem.get("original_user_id")
        uid = int(elem.get("user_id"))
        node_data = GoodreadsDataPreparser.NodeData(original_uid, GoodreadsDataPreparser.interaction_types)
        #newnode = NetworkNode(uid, has_friends=True, has_interactions=True,
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
        for (sender_id, receiver_id), edgedata in edges_iter:
            #sender_id, receiver_id = k
            if sender_id < len(self.nodes) and receiver_id <len(self.nodes):
                self.nodes[sender_id].add_friend(receiver_id, self.nodes[receiver_id], None)
                self.nodes[receiver_id].add_friend(sender_id, self.nodes[sender_id], None)
            #else:
            #    print "error: node is missing to create edge"
            #return self.edges
        print "All edges stored"


if __name__ == "__main__":
    x = GoodreadsDataPreparser(
        node_filename='/home/asharma/datasets/goodreads/goodreads.300k.users.xml',
        item_filename='/home/asharma/datasets/goodreads/goodreads.300k.items.xml',
        interaction_filename='/home/asharma/datasets/goodreads/goodreads.300k.collections.txt',
        edge_filename="/home/asharma/datasets/goodreads/goodreads.300k.edges.xml"
    )
    u_dict = x.read_nodes_file()
    print u_dict["1"]


