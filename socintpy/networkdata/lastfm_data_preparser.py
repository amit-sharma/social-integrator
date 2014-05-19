from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
from socintpy.store.sqlite_store import SqliteStore


class LastfmDataPreparser(NetworkDataPreparser):
    interact_types_dict = {"listen": 0, "love": 1, "ban": 2}
    def __init__(self, nodes_store_path, edges_store_path):
        NetworkDataPreparser.__init__(self)
        self.nodes_db = SqliteStore(nodes_store_path)
        self.edges_db = SqliteStore(edges_store_path)
        self.interaction_types = sorted(LastfmDataPreparser.interact_types_dict.values())

    def get_all_data(self):
        self.read_nodes()
        self.read_friends()

    def read_nodes(self):
        i = 0
        for temp_key, data_dict in self.nodes_db.iteritems():
            user_id = temp_key
            print data_dict
            new_netnode = self.create_network_node(user_id, should_have_friends=True, should_have_interactions=True, node_data={'interactions':self.interaction_types})
            for trackdict in data_dict['lovedtracks']:
                new_netnode.add_interaction(self.interact_types_dict["love"], trackdict['mbid'], {'created_time': trackdict['date']['uts']})

            for trackdict in data_dict['bannedtracks']:
                new_netnode.add_interaction(self.interact_types_dict["ban"], trackdict['mbid'], {'created_time': trackdict['date']['uts']})

            for trackdict in data_dict['tracks']:
                if '@attr' in trackdict:
                    #pprint(trackdict)
                    if 'nowplaying' in trackdict['@attr']:
                        print "Now playing track, ignoring!"
                        continue

                new_netnode.add_interaction(self.interact_types_dict["listen"], trackdict['mbid'], {'created_time': trackdict['date']['uts']})

            self.nodes[user_id] = new_netnode

            #new_netnode.add_interaction()
            i+= 1
            if i >5:
                break

    def read_friends(self):
        for edge_id, data_dict in self.edges_db.iteritems():
            source_user = data_dict['source']
            target_user = data_dict['target']
            if target_user not in self.nodes:
                self.nodes[target_user] = self.create_network_node(target_user, should_have_friends=False, should_have_interactions=False, node_data={'interactions':self.interaction_types})
            self.nodes[source_user].add_friend(target_user, self.nodes[target_user], None)
            #break




