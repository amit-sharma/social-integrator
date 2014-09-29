#import pyximport; pyximport.install()
from socintpy.cythoncode.cnetwork_node import CNetworkNode
from socintpy.networkdata.pynetwork_node import PyNetworkNode
import random
import codecs
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
        self.total_num_items = None
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
    
    def compute_store_total_num_items(self):
        items_all = []
        users_with_zero_interacts = 0
        for v in self.get_nodes_iterable(should_have_interactions=True):
            items_list = v.get_all_items_interacted_with()
            if len(items_list)==0:
                users_with_zero_interacts += 1
            items_all.extend(items_list)
        items_all = set(items_all)
        print "Total number of users(including non-core) with zero interacts (across all types)", users_with_zero_interacts
        return(items_all)    

    def get_friends_nodes(self, node):
        return [self.nodes[fid] for fid in node.get_friend_ids()]

    def get_total_num_nodes(self):
        return len(self.nodes)-1
    
    def get_node_ids(self):
        return range(1,len(self.nodes))

    """
    def get_item_ids(self):
        return range(1, len(self.items))
    """
    def get_max_item_id(self):
        return max(self.item_ids_list)
    # This is different because in some cases, the max_id may be greater than total
    # number of items
    def get_total_num_items(self):
        return self.total_num_items
    

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
   
    def store_ego_dataset(self, filepath):
        node_count = 0
        f = codecs.open(filepath+'egodata_'+str(node_count)+'.tsv', encoding="utf-8", mode='w')
        f.write("# Interacts:"+repr(self.interact_types_dict.items())+'\n')
        for node in self.get_nodes_iterable():
            f.write(unicode(node.uid)+' '+unicode(node.should_have_friends)+"\n")
            
            fids = node.get_friend_ids()
            if len(fids)>0:
                for fid in fids:
                    f.write(unicode(fid)+' ')
            else:
                f.write('None')
            f.write('\n')
            
            for interact_type in sorted(self.interact_types_dict.values()):
                interacts = node.get_items_interacted_with(interact_type, return_timestamp=True)
                if len(interacts) >0:
                    for item_id, timestamp in interacts:
                        f.write(unicode(item_id)+':'+unicode(timestamp)+' ')
                else:
                    f.write("None")
                f.write('\n')

            node_count += 1
            if node_count % 10000 == 0:
                f.close()
                print "Wrote", node_count, "nodes"
                f = codecs.open(filepath+'egodata_'+str(node_count)+'.tsv', encoding="utf-8", mode='w')
                f.write("# Interacts:"+repr(self.interact_types_dict.items())+'\n')
        f.close()

        f_nodemap = codecs.open(filepath+'nodemap'+'.tsv', encoding="utf-8", mode='w')
        for node_name, node_id in self.uid_dict.iteritems():
            f_nodemap.write(unicode(node_name) + ' ' + unicode(node_id) + '\n')
        f_nodemap.close()
        
        f_itemmap = codecs.open(filepath+'itemmap'+'.tsv', encoding="utf-8", mode='w')
        for item_name, item_id in self.itemid_dict.iteritems():
            f_itemmap.write(unicode(item_name) + ' ' + unicode(item_id) + '\n')
        f_itemmap.close()
        return
        

    def load_dataset(self):
        with open(self.datadir, 'rb') as pkl_file:
            self.nodes = pickle.load(pkl_file)


