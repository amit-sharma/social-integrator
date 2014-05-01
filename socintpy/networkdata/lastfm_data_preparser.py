from pprint import pprint
from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
from socintpy.store.sqlite_store import SqliteStore


class LastfmDataPreparser(NetworkDataPreparser):
    def __init__(self, nodes_store_path, edges_store_path):
        NetworkDataPreparser.__init__(self)
        self.nodes_db = SqliteStore(nodes_store_path)
        self.edges_db = SqliteStore(edges_store_path)
        self.interaction_types = ["listen", "love", "ban"]

    def get_all_data(self):
        self.read_nodes()

    def read_nodes(self):
        for uid, value in self.nodes_db.iteritems():
            self.nodes[uid] = NetworkNode(uid, is_core=True, node_data={'interactions':self.interaction_types})
            pprint(value)
            break




