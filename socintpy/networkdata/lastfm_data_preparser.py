from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
from socintpy.store.sqlite_store import SqliteStore
from collections import namedtuple

class LastfmDataPreparser(NetworkDataPreparser):
    interact_types_dict = {"listen": 0, "love": 1, "ban": 2}
    NodeData = namedtuple('NodeData', 'original_uid interaction_types')
    InteractData = namedtuple('InteractData', 'item_id original_item_id created_time')
    EdgeData= namedtuple('EdgeData', 'receiver_id')
    def __init__(self, nodes_store_path, edges_store_path, node_impl):
        NetworkDataPreparser.__init__(self, node_impl)
        self.nodes_db = SqliteStore(nodes_store_path)
        self.edges_db = SqliteStore(edges_store_path)
        self.interaction_types = sorted(LastfmDataPreparser.interact_types_dict.values())
        self.node_index = 0
        self.item_index = 0

    def get_all_data(self):
        uid_dict = self.read_nodes()
        self.read_friends(uid_dict)

    def read_nodes(self):
        uid_dict = {}
        itemid_dict = {}
        self.nodes.append(None)
        for temp_key, data_dict in self.nodes_db.iteritems():
            self.node_index += 1
            user_id = self.node_index
            #print data_dict
            uid_dict[temp_key] = user_id
            node_data = LastfmDataPreparser.NodeData(original_uid=data_dict['name'], interaction_types=self.interaction_types)

            new_netnode = self.create_network_node(user_id, should_have_friends=True, should_have_interactions=True, node_data=node_data)
            self.store_interactions_by_type(new_netnode, self.interact_types_dict['love'], data_dict['lovedtracks'], itemid_dict)
            self.store_interactions_by_type(new_netnode, self.interact_types_dict['ban'], data_dict['bannedtracks'], itemid_dict)
           
            recent_tracks_list = [] 
            for trackdict in data_dict['tracks']:
                if '@attr' in trackdict:
                    #pprint(trackdict)
                    if 'nowplaying' in trackdict['@attr']:
                        print "Now playing track, ignoring!"
                        continue
                recent_tracks_list.append(trackdict)
            self.store_interactions_by_type(new_netnode, self.interact_types_dict['listen'], recent_tracks_list, itemid_dict)

            self.nodes.insert(self.node_index, new_netnode)

        return uid_dict

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
            ilist.append(LastfmDataPreparser.InteractData(item_id=item_id, original_item_id=trackdict['mbid'],created_time=trackdict['date']['uts']))
        netnode.store_interactions(interact_type, ilist)
        return

    def read_friends(self, uid_dict):
        flist = []
        prev_source_id = None
        for edge_id, data_dict in self.edges_db.iteritems(sorted=True):
            source_user = data_dict['source']
            target_user = data_dict['target']
            source_id = uid_dict[source_user]
            
            if target_user in uid_dict:
                friend_id = uid_dict[target_user]
            else:
                self.node_index += 1
                friend_id = self.node_index
                uid_dict[target_user] = friend_id
                node_data = LastfmDataPreparser.NodeData(original_uid=target_user, interaction_types=self.interaction_types)
                self.nodes.insert(friend_id, self.create_network_node(friend_id, should_have_friends=False, should_have_interactions=False, node_data=node_data))
            
            if prev_source_id is not None and prev_source_id != source_id:
                self.nodes[prev_source_id].store_friends(flist)
                flist = []
                
            flist.append(LastfmDataPreparser.EdgeData(receiver_id=friend_id))
            prev_user = source_id
            #break
        self.nodes[prev_source_id].store_friends(flist)
        return




