from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
from socintpy.store.sqlite_store import SqliteStore
from collections import namedtuple
import logging

class LastfmDataPreparser(NetworkDataPreparser):
    interact_types_dict = {"listen": 0, "love": 1, "ban": 2}
    NodeData = namedtuple('NodeData', 'original_uid interaction_types')
    InteractData = namedtuple('InteractData', 'item_id original_item_id timestamp rating')
    EdgeData= namedtuple('EdgeData', 'receiver_id')

    def __init__(self, nodes_store_path, edges_store_path, node_impl):
        NetworkDataPreparser.__init__(self, node_impl)
        self.nodes_db = SqliteStore(nodes_store_path)
        self.edges_db = SqliteStore(edges_store_path)
        self.interaction_types = sorted(LastfmDataPreparser.interact_types_dict.values())
        self.node_index = 0
        self.item_index = 0
        self.itemid_dict = {} 
        self.MAX_NODES_TO_READ = 100

    def get_all_data(self):
        uid_dict = self.read_nodes()
        friends_tostore = self.read_friends(uid_dict)
        self.store_friends_interactions(friends_tostore)

    def read_nodes(self):
        uid_dict = {}
        self.nodes.append(None)
        for temp_key, data_dict in self.nodes_db.iteritems():
            if self.node_index % 100 == 0:
                print "Storing information about ", self.node_index
            if self.node_index == self.MAX_NODES_TO_READ:
                break
            self.node_index += 1
            user_id = self.node_index
            print temp_key#data_dict
            uid_dict[temp_key] = user_id
            node_data = LastfmDataPreparser.NodeData(original_uid=data_dict['name'], interaction_types=self.interaction_types)

            new_netnode = self.create_network_node(user_id, should_have_friends=True, should_have_interactions=True, node_data=node_data)

            self.nodes.insert(user_id, new_netnode)
            self.store_interactions(user_id, data_dict)

        return uid_dict

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
                self.item_index += 1
            ilist.append(LastfmDataPreparser.InteractData(item_id=item_id, original_item_id=trackdict['mbid'],timestamp=trackdict['date']['uts'], rating=1))
        netnode.store_interactions(interact_type, ilist)
        return

    def read_friends(self, uid_dict):
        flist = []
        prev_source_id = None
        count_nodes_stored = 0
        max_core_node_index = self.node_index
        friend_uid_dict = {}
        for edge_id, data_dict in self.edges_db.iteritems(sorted=False):
            source_user = data_dict['source']
            target_user = data_dict['target']
            #print source_user, target_user
            # assuming edges are always stored in same order as the nodes
            if source_user not in uid_dict:
                print edge_id,count_nodes_stored 
                break
            if uid_dict[source_user] > max_core_node_index:
                break
            source_id = uid_dict[source_user]
            
            if target_user in uid_dict:
                friend_id = uid_dict[target_user]
            elif target_user in friend_uid_dict:
                friend_id = friend_uid_dict[target_user]
            else:
                self.node_index += 1
                friend_id = self.node_index
                friend_uid_dict[target_user] = friend_id
                node_data = LastfmDataPreparser.NodeData(original_uid=target_user, interaction_types=self.interaction_types)
                self.nodes.insert(friend_id, self.create_network_node(friend_id, should_have_friends=False, should_have_interactions=True, node_data=node_data))
            
            if prev_source_id is not None and prev_source_id != source_id:
                #print prev_source_id, self.MAX_NODES_TO_READ, "Yo"
                self.nodes[prev_source_id].store_friends(flist)
                count_nodes_stored += 1
                flist = []
                
            flist.append(LastfmDataPreparser.EdgeData(receiver_id=friend_id))
            prev_source_id = source_id
        
        self.nodes[prev_source_id].store_friends(flist)
        return friend_uid_dict
    
    def store_friends_interactions(self, friend_ids):
        #sfr_uids = sorted(friend_uids.iteritems(), key=lambda x: x[1] )
        #fr_index = 0
        for orig_friend_id, friend_id in friend_ids.iteritems():
            if orig_friend_id in self.nodes_db:
                data_dict = self.nodes_db[orig_friend_id]
        #for temp_key, data_dict in self.nodes_db.iteritems():
        #    if temp_key in friend_uids:
                self.store_interactions(friend_id, data_dict)

        return 





