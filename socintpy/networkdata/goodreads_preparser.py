#import xml.etree.ElementTree as etree
from lxml import etree
from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
import socintpy.util.utils as utils


class GoodreadsDataPreparser(NetworkDataPreparser):
    def __init__(self, data_path):
        NetworkDataPreparser.__init__(self)
        self.datadir = data_path
        self.nodes_filename = data_path + "goodreads.300k.users.xml"
        self.interaction_filename = data_path + "goodreads.300k.collections.txt"
        #self.nodes = self.read_nodes_file(node_filename)
        self.items_filename = data_path + "goodreads.300k.items.xml"
        self.edges_filename = data_path + "goodreads.300k.edges.xml"

        self.interactions_filename = data_path + "goodreads.300k.collections.txt"
        #self.interaction_types = ["rate"]

        #self.edges = self.read_edges_file(edge_filename)


    def get_all_data(self):
        self.read_nodes_file()
        print self.nodes["1"]
        self.read_items_file()
        self.read_edges_file()


    def read_items_file(self):
        context = etree.iterparse(self.items_filename, events=('end',), tag="item")
        self.items = utils.fast_iter(context, lambda elem: (elem.get('item_id'), dict(elem.attrib)))
        return self.items


    def read_nodes_file(self):
        context = etree.iterparse(self.nodes_filename, events=('end',), tag="user")
        self.nodes = utils.fast_iter(context, lambda elem: (elem.get("user_id"), dict(elem.attrib)))
        return self.edges

    def read_interactions_file(self, filename):
        interactions_dict = {}
        inter_file = open(filename)
        for line in inter_file:
            cols = line.strip(" \n\t").split(",")
            user_id = cols[0]
            item_id = cols[1]
            timestamp = cols[2]
            rating = cols[3]
            new_interaction = {'user_id': user_id, 'item_id': item_id,
                               'timestamp': timestamp, 'rating': rating}
            interactions_dict[user_id + item_id] = new_interaction
        return interactions_dict


    @staticmethod
    def handle_edges(elem):
        sender_id = elem.get("sender_id")
        receiver_id = elem.get("receiver_id")
        if sender_id < receiver_id:
            key = sender_id + receiver_id
        else:
            key = receiver_id + sender_id
        return key, dict(elem.attrib)


    def read_edges_file(self):
        context = etree.iterparse(self.edges_filename, events=('end',), tag="edge")
        self.edges = utils.fast_iter(context, self.handle_edges)
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


