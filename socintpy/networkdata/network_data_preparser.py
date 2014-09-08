#import pyximport; pyximport.install()
from socintpy.cythoncode.cnetwork_node import CNetworkNode
from socintpy.networkdata.pynetwork_node import PyNetworkNode
import random
import cPickle as pickle

class NetworkDataPreparser():
    def __init__(self, node_impl, data_path, min_interactions=0, min_friends=1, 
                 max_core_nodes=None, store_dataset = False):
        self.nodes = []
        self.items = []
        self.edges = []
        self.impl_type = node_impl
        self.min_interactions = min_interactions
        self.min_friends = min_friends
        self.nodes_to_fetch = max_core_nodes
        self.store_dataset = store_dataset
        
        self.load_from_saved_dataset = False
        if data_path.endswith(".pkl"):
            self.load_from_saved_dataset = True
        # Workaround because namedtuple factory does not behave nicely with 
        # nested namedtuple
        self.named_tuple_dict = {'InteractData': self.__class__.InteractData,
                                 'EdgeData': self.__class__.EdgeData,
                                 'NodeData': self.__class__.NodeData,
                                 'ItemData': self.__class__.ItemData}

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

    def select_subset_nodes(self):
        # TODO For now, we do not update self.items list
        core_nodes = self.get_nodes_list(should_have_friends=True)
        # array of indices of the core_nodes array. Not really node_ids.
        selected_indices = random.sample(xrange(len(core_nodes)), self.nodes_to_fetch)        
        selected_nodes = [] # new list that will replace self.nodes
        counter = 0
        selected_nodes.insert(counter, None)
        counter += 1
        id_remap = {} # re-mapping the ids for nodes
        # add fixed number of randomly selected core nodes
        for index in selected_indices:
            id_remap[core_nodes[index].uid] = counter
            core_nodes[index].uid = counter
            selected_nodes.insert(counter, core_nodes[index])
            counter += 1
        
        # now modifying the node-ids in the friends list of selected core nodes
        for node in selected_nodes[1:]:
            friend_ids = node.get_friend_ids()
            flist = []
            for fr_id in friend_ids:
                if fr_id not in id_remap:
                    id_remap[fr_id] = counter
                    # change should_have_friends and destroy friends lists of 
                    # hitherto core-nodes
                    #print fr_id, len(self.nodes)
                    self.nodes[fr_id].remove_friends()
                    selected_nodes.insert(counter, self.nodes[fr_id])
                    counter += 1
                flist.append(self.__class__.EdgeData(receiver_id=id_remap[fr_id]))
            node.store_friends(flist)
        #print counter, "counter"
        for node in self.nodes[1:]:
            if node.uid not in id_remap:
                del node
        self.nodes = selected_nodes
 
    def dump_dataset(self):
        if self.datadir.endswith('/'):
            file_path_arr = self.datadir.rsplit('/', 2)
        else:
            file_path_arr = self.datadir.rsplit('/', 1)
        file_path = file_path_arr[0]
        filename = str(file_path + "/dataset_cutoff" + str(self.cutoff_rating) + 
            "_maxcorenodes_" + str(self.nodes_to_fetch) + ".pkl")
        with open(filename, 'wb') as outf:
          pickle.dump(self.nodes, outf, -1)
    
    def load_dataset(self):
        with open(self.datadir, 'rb') as pkl_file:
            self.nodes = pickle.load(pkl_file)


