from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
from socintpy.store.sqlite_store import SqliteStore
from collections import namedtuple
import logging, os


def handle_os_walk(os_error):
    raise

class LastfmDataPreparser(NetworkDataPreparser):
    interact_types_dict = {"listen": 0, "love": 1, "ban": 2}
    NodeData = namedtuple('NodeData', 'original_uid interaction_types')
    InteractData = namedtuple('InteractData', 'item_id original_item_id timestamp rating')
    EdgeData= namedtuple('EdgeData', 'receiver_id')
    ItemData = namedtuple('ItemData', 'item_id') # not used currently

    def __init__(self, data_path, node_impl, cutoff_rating, max_core_nodes, 
                 store_dataset):
        NetworkDataPreparser.__init__(self, node_impl, data_path,
                                      max_core_nodes=max_core_nodes, 
                                      store_dataset=store_dataset)
        self.datadir = data_path
        #nodes_store_path = data_path + "lastfm_nodes.db"
        #edges_store_path = data_path + "lastfm_edges.db"
        self.nodes_db_list = []
        self.edges_db_list = []
        self.interaction_types = sorted(LastfmDataPreparser.interact_types_dict.values())
        self.node_index = 0
        self.item_index = 1
        self.items.insert(0, None)
        self.itemid_dict = {} 
        
        self.MAX_NODES_TO_READ = 100000 
        self.cutoff_rating = None
       
        self.nodes_files = []
        self.edges_files = []
        self.uid_dict = {}
        self.friend_uid_dict = {}

    def get_all_data(self):
        for root, dirs, files in os.walk(self.datadir, onerror=handle_os_walk):
            for filename in files:
                fname_chunks = filename.split(".")
                uname = fname_chunks[0]
                filepath = os.path.join(root, ".".join(fname_chunks[:2]))
                if uname.endswith("_nodes"):
                    self.nodes_files.append(filepath)
                    nodes_db_obj = SqliteStore(filepath)
                    self.nodes_db_list.append(nodes_db_obj)
                elif uname.endswith("_edges"):
                    self.edges_files.append(filepath)
                    self.edges_db_list.append(SqliteStore(filepath))
       
        #print self.edges_files
        #print self.nodes_files
        self.nodes.append(None)
        for node_db in self.nodes_db_list:
            uid_dict = self.read_nodes(node_db)
        max_core_node_index = self.node_index
        print max_core_node_index 

        for edge_db in self.edges_db_list:
            friends_tostore = self.read_friends(edge_db, max_core_node_index)
            print "Done once"
        
        self.store_friends_interactions()

    def read_nodes(self, nodes_db):
        print "reading nodes"
        counter_node = 0 
        for temp_key, data_dict in nodes_db.iteritems():
            if self.node_index % 100 == 0:
                print "Storing information about ", self.node_index
            #if counter_node == self.MAX_NODES_TO_READ:
            #    break
            self.node_index += 1
            counter_node += 1
           
            user_id = self.node_index
            #print temp_key#data_dict
            self.uid_dict[temp_key] = user_id
            node_data = LastfmDataPreparser.NodeData(original_uid=data_dict['name'], interaction_types=self.interaction_types)

            new_netnode = self.create_network_node(user_id, should_have_friends=True, should_have_interactions=True, node_data=node_data)

            self.nodes.insert(user_id, new_netnode)
            self.store_interactions(user_id, data_dict)
            
        return self.uid_dict

    def read_friends(self, edge_db, max_core_node_index):
        flist = []
        prev_source_id = None
        #count_nodes_stored = 0
        for edge_id, data_dict in edge_db.iteritems(sorted=False):
            source_user = data_dict['source']
            target_user = data_dict['target']
            #print source_user, target_user
            # assuming edges are always stored in same order as the nodes
            if source_user not in self.uid_dict:
                #print edge_id,count_nodes_stored 
                break
            if self.uid_dict[source_user] > max_core_node_index:
                break
            source_id = self.uid_dict[source_user]
            
            if target_user in self.uid_dict:
                friend_id = self.uid_dict[target_user]
            elif target_user in self.friend_uid_dict:
                friend_id = self.friend_uid_dict[target_user]
            else:
                self.node_index += 1
                friend_id = self.node_index
                self.friend_uid_dict[target_user] = friend_id
                node_data = LastfmDataPreparser.NodeData(original_uid=target_user, interaction_types=self.interaction_types)
                self.nodes.insert(friend_id, self.create_network_node(friend_id, should_have_friends=False, should_have_interactions=True, node_data=node_data))
            
            if prev_source_id is not None and prev_source_id != source_id:
                #print prev_source_id, self.MAX_NODES_TO_READ, "Yo"
                self.nodes[prev_source_id].store_friends(flist)
                #count_nodes_stored += 1
                flist = []
                
            flist.append(LastfmDataPreparser.EdgeData(receiver_id=friend_id))
            prev_source_id = source_id
        
        self.nodes[prev_source_id].store_friends(flist)
        return self.friend_uid_dict
    
    def store_interactions(self, user_id, data_dict):
        self.store_interactions_by_type(self.nodes[user_id], self.interact_types_dict['love'], data_dict['lovedtracks'], self.itemid_dict)
        self.store_interactions_by_type(self.nodes[user_id], self.interact_types_dict['ban'], data_dict['bannedtracks'], self.itemid_dict)
       
        recent_tracks_list = [] 
        for trackdict in data_dict['tracks']:
            if '@attr' in trackdict:
                #pprint(trackdict)
                if 'nowplaying' in trackdict['@attr']:
                    logging.warn("Now playing track, ignoring %s,%d!" %(data_dict['name'], user_id))
                    continue
            recent_tracks_list.append(trackdict)
        self.store_interactions_by_type(self.nodes[user_id], self.interact_types_dict['listen'], recent_tracks_list, self.itemid_dict)

    def store_interactions_by_type(self,netnode, interact_type, interactions_list, itemid_dict):
        ilist = []
        for trackdict in interactions_list:
            orig_item_id = trackdict['mbid']
            if orig_item_id in itemid_dict:
                item_id = itemid_dict[orig_item_id]
            else:
                item_id = self.item_index
                itemid_dict[orig_item_id] = item_id
                self.items.insert(self.item_index, (orig_item_id,)) 
                self.item_index += 1
            ilist.append(LastfmDataPreparser.InteractData(item_id=item_id, original_item_id=trackdict['mbid'],timestamp=trackdict['date']['uts'], rating=1))
            #print "%s" %trackdict['date']['uts']
        netnode.store_interactions(interact_type, ilist)
        return

    
    def store_friends_interactions(self):
        #sfr_uids = sorted(friend_uids.iteritems(), key=lambda x: x[1] )
        counter = 0 
        for orig_friend_id, friend_id in self.friend_uid_dict.iteritems():
            if counter % 1000 == 0:
                print "Trying out friend numbered ", counter
            counter += 1 
            for node_db in self.nodes_db_list:
                if orig_friend_id in node_db:
                    data_dict = node_db[orig_friend_id]
        #for temp_key, data_dict in self.nodes_db.iteritems():
        #    if temp_key in friend_uids:
                    self.store_interactions(friend_id, data_dict)
                    break

        return 





