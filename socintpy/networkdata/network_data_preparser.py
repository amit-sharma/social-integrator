from socintpy.networkdata.network_node import NetworkNode
from socintpy.networkdata.pynetwork_node import PyNetworkNode


class NetworkDataPreparser():
    def __init__(self, node_impl):
        self.nodes = []
        self.items = []
        self.edges = []
        self.impl_type = node_impl

    def read_nodes(self):
        raise NotImplementedError("NetworkDataPreparser: read_nodes is not implemented.")

    def get_nodes_iterable(self, should_have_friends= False, should_have_interactions=False):
        for v in self.nodes[1:]:
            if should_have_friends and not v.has_friends:
                continue
            if should_have_interactions and not v.has_interactions:
                continue
            yield v

    def get_nonfriends_iterable(self, node):
        node_friend_ids = [f[0] for f in node.friends]
        for v in self.nodes[1:]:
            if v.uid not in node_friend_ids and v.uid != node.uid:
                yield v

    def get_total_num_nodes(self):
        return len(self.nodes)-1
    
    def get_node_ids(self):
        return range(1,len(self.nodes))

    def create_network_node(self, uid, has_friends=True, has_interactions=True, node_data=None):
        if self.impl_type == "cython":
            return NetworkNode(uid, has_friends=has_friends, has_interactions=has_interactions, node_data=node_data)
        else:
            return PyNetworkNode(uid, has_friends, has_interactions, node_data)
