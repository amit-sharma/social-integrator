import xml.etree.ElementTree as etree
from socintpy.networkdata.network_data_preparser import NetworkDataPreparser


class GoodreadsDataPreparser(NetworkDataPreparser):

    def __init__(self, data_path):
        self.datadir = data_path
        self.nodes_filename = data_path + "goodreads.300k.users.xml"
        self.interaction_filename = data_path + "goodreads.300k.collections.txt"
        #self.nodes = self.read_nodes_file(node_filename)
        #self.items = self.read_items_file(item_filename)
        #self.interactions = self.read_interactions_file(interaction_filename)
        #self.interaction_types = ["rate"]

        #self.edges = self.read_edges_file(edge_filename)

    def get_all_data(self):
        self.read_nodes_file()

    def read_items_file(self, filename):
        items_dict = {}
        context = iter(etree.iterparse(filename, events=('start', 'end')))
        event, root = context.next()

        for event, elem in context:
            if elem.tag == "item" and event == "start":
                #print elem.get("item_id"), elem.get("original_item_id"),elem.get("average_rating"), elem.get('popularity'),elem.get('category')
                items_dict[elem.get('item_id')] = dict(elem.attrib)

                root.clear()
        return items_dict

    def read_nodes_file(self):
        users_dict = {}
        context = iter(etree.iterparse(self.nodes_filename, events=('start', 'end')))
        event, root = context.next()

        for event, elem in context:
            if elem.tag == "user" and event == "start":
                #print elem.get("user_id"), elem.get("original_user_id")
                users_dict[elem.get("user_id")] = dict(elem.attrib)
                root.clear()
        return users_dict

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

    def read_edges_file(self, filename):
        edges_dict = {}
        for event, elem in etree.iterparse(filename, events=('start', 'end')):
            if elem.tag == "edge" and event == "start":
                #print elem.get("user_id"), elem.get("original_user_id")
                sender_id = elem.get("sender_id")
                receiver_id = elem.get("receiver_id")
                if sender_id < receiver_id:
                    key = sender_id + receiver_id
                else:
                    key = receiver_id + sender_id
                edges_dict[key] = dict(elem.attrib)
            elem.clear()
        return edges_dict


if __name__ == "__main__":
    x = GoodreadsDataPreparser(
        node_filename='/home/asharma/datasets/goodreads/goodreads.300k.users.xml',
        item_filename='/home/asharma/datasets/goodreads/goodreads.300k.items.xml',
        interaction_filename='/home/asharma/datasets/goodreads/goodreads.300k.collections.txt',
        edge_filename="/home/asharma/datasets/goodreads/goodreads.300k.edges.xml"
    )
    u_dict=x.read_nodes_file()
    print u_dict["1"]



