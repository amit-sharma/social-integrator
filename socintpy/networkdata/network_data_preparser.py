#import pyximport; pyximport.install()
from socintpy.cythoncode.cnetwork_node import CNetworkNode
from socintpy.networkdata.pynetwork_node import PyNetworkNode



class NetworkDataPreparser():
    def __init__(self, node_impl, min_interactions=0, min_friends=1):
        self.nodes = []
        self.items = []
        self.edges = []
        self.impl_type = node_impl
        self.min_interactions = min_interactions
        self.min_friends = min_friends

    def read_nodes(self):
        raise NotImplementedError("NetworkDataPreparser: read_nodes is not implemented.")

    def get_nodes_iterable(self, should_have_friends= False, should_have_interactions=False):
        for v in self.nodes[1:]:
            if should_have_friends and not v.should_have_friends:
                continue
            if should_have_interactions and not v.should_have_interactions:
                continue
            yield v

    def get_nodes_list(self, should_have_friends=False, should_have_interactions=False):
        ret_list = []
        for v in self.nodes[1:]:
            if should_have_friends and not v.should_have_friends:
                continue
            if should_have_interactions and not v.should_have_interactions:
                continue
            ret_list.append(v)
        return ret_list

    def get_all_nodes(self):
        return self.nodes[1:]

    def get_node_objs(self, indices):
        return [self.nodes[i] for i in indices]

    def get_nonfriends_iterable(self, node):
        node_friend_ids = node.get_friend_ids()
        for v in self.nodes[1:]:
            if v.uid not in node_friend_ids and v.uid != node.uid:
                yield v
    
    def get_nonfriends_nodes(self, node):
        ret = []
        node_friend_ids = node.get_friend_ids()
        for v in self.nodes[1:]:
            if v.uid not in node_friend_ids and v.uid != node.uid:
                ret.append(v)
        return ret

    def get_friends_iterable(self, node):
        for fid in node.get_friend_ids():
            yield self.nodes[fid]
    
    def get_friends_nodes(self, node):
        return [self.nodes[fid] for fid in node.get_friend_ids()]

    def get_total_num_nodes(self):
        return len(self.nodes)-1
    
    def get_node_ids(self):
        return range(1,len(self.nodes))

    def get_item_ids(self):
        return range(1, len(self.items))

    def get_total_num_items(self):
        return len(self.items)-1

    def create_network_node(self, uid, should_have_friends=True, should_have_interactions=True, node_data=None):
        if self.impl_type == "cython":
            return CNetworkNode(uid, should_have_friends=should_have_friends, should_have_interactions=should_have_interactions, node_data=node_data)
        else:
            return PyNetworkNode(uid, should_have_friends, should_have_interactions, node_data)
