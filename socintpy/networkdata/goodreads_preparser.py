#import xml.etree.ElementTree as etree
from lxml import etree
from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
import socintpy.util.utils as utils


class GoodreadsDataPreparser(NetworkDataPreparser):
    interaction_types = ["rate"]
    def __init__(self, data_path):
        NetworkDataPreparser.__init__(self)
        self.datadir = data_path
        self.nodes_filename = data_path + "goodreads.300k.users.xml"
        self.items_filename = data_path + "goodreads.300k.items.xml"
        self.edges_filename = data_path + "goodreads.300k.edges.xml"

        self.interactions_filename = data_path + "goodreads.300k.collections.txt"
        self.interaction_types = ["rate"]



    def get_all_data(self):
        self.read_nodes_file()
        print self.nodes["1"]
        self.read_items_file()
        #self.read_edges_file()
        self.read_interactions_file()


    def read_items_file(self):
        context = etree.iterparse(self.items_filename, events=('end',), tag="item")
        self.items = utils.fast_iter(context, lambda elem: (elem.get('item_id'), dict(elem.attrib)))
        return self.items


    def read_nodes_file(self):
        context = etree.iterparse(self.nodes_filename, events=('end',), tag="user")
        nodes_iter = utils.fast_iter(context, self.handle_nodes)
        for k, v in nodes_iter:
            self.nodes[k] = v
        return self.nodes

    def read_interactions_file(self):
        #interactions_dict = {}
        inter_file = open(self.interactions_filename)
        counter = 0
        for line in inter_file:
            cols = line.strip(" \n\t").split(",")
            user_id = cols[0]
            item_id = cols[1]
            timestamp = cols[2]
            rating = cols[3]
            new_interaction = {'user_id': user_id, 'item_id': item_id,
                               'timestamp': timestamp, 'rating': rating}
            #interactions_dict[user_id + item_id] = new_interaction
            self.nodes[user_id].add_interaction("rate", item_id, new_interaction)
            counter += 1
            if counter > 100:
                break

    @staticmethod
    def handle_nodes(elem):
        uid = elem.get("user_id")
        newnode= NetworkNode(uid, has_friends=True, has_interactions=True, node_data={'interactions': GoodreadsDataPreparser.interaction_types})
        return (uid, newnode)

    @staticmethod
    def handle_edges(elem):
        sender_id = elem.get("sender_id")
        receiver_id = elem.get("receiver_id")
        if sender_id < receiver_id:
            key = sender_id + receiver_id
        else:
            key = receiver_id + sender_id
        return (sender_id, receiver_id), dict(elem.attrib)


    def read_edges_file(self):
        context = etree.iterparse(self.edges_filename, events=('end',), tag="edge")
        edges_iter = utils.fast_iter(context, self.handle_edges)
        for k, v in edges_iter:
            sender_id, receiver_id = k
            self.nodes[sender_id].add_friend(receiver_id, self.nodes[receiver_id], None)
        return self.edges


    if __name__ == "__main__":
        x = GoodreadsDataPreparser(
            node_filename='/home/asharma/datasets/goodreads/goodreads.300k.users.xml',
            item_filename='/home/asharma/datasets/goodreads/goodreads.300k.items.xml',
            interaction_filename='/home/asharma/datasets/goodreads/goodreads.300k.collections.txt',
            edge_filename="/home/asharma/datasets/goodreads/goodreads.300k.edges.xml"
        )
        u_dict = x.read_nodes_file()
        print u_dict["1"]


