from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
from socintpy.store.sqlite_store import SqliteStore
from socintpy.util.unicode_csv import unicode_csv_reader
from collections import namedtuple
import logging, os, csv
import threading

def handle_os_walk(os_error):
    raise

class LastfmDataPreparserCSV(NetworkDataPreparser):
    interact_types_dict = {"listen": 0, "love": 1, "ban": 2}
    #interact_types_dict= {"ban":0}
    NodeData = namedtuple('NodeData', 'original_uid interaction_types')
    InteractData = namedtuple('InteractData', 'item_id original_item_id timestamp rating')
    EdgeData= namedtuple('EdgeData', 'receiver_id')
    ItemData = namedtuple('ItemData', 'item_id') # not used currently

    def __init__(self, data_path, node_impl, cutoff_rating, max_core_nodes, 
                 store_dataset, use_artists):
        NetworkDataPreparser.__init__(self, node_impl, data_path,
                                      max_core_nodes=max_core_nodes, 
                                      store_dataset=store_dataset)
        self.datadir = data_path
        self.nodes_db_list = []
        self.edges_db_list = []
        self.interaction_types = sorted(LastfmDataPreparserCSV.interact_types_dict.values())
        self.node_index = 0
        self.item_index = 1
        self.items.insert(0, None)
        self.itemid_dict = {} 
        
        self.cutoff_rating = None
       
        self.nodes_files = []
        self.edges_files = []
        self.interacts_files = {}
        for interact_name in self.interact_types_dict.keys():
            self.interacts_files[interact_name] = []
        self.uid_dict = {}
        self.friend_uid_dict = {}
        self.use_artists = use_artists

    def get_all_data(self):
        for root, dirs, files in os.walk(self.datadir, onerror=handle_os_walk):
            for filename in files:
                fname_chunks = filename.split(".")
                uname = fname_chunks[0]
                uname_lastchunk = uname.split("_")[-1]
                filepath = os.path.join(root, ".".join(fname_chunks))
                if filename=="lastfm_nodes.tsv":
                    self.nodes_files.append(filepath)
                    nodes_db_obj = unicode_csv_reader(open(filepath))
                    self.nodes_db_list.append(nodes_db_obj)
                elif filename == "lastfm_edges.tsv":
                    self.edges_files.append(filepath)
                    self.edges_db_list.append(unicode_csv_reader(open(filepath)))
                elif uname_lastchunk in self.interact_types_dict.keys():
                    self.interacts_files[uname_lastchunk].append(unicode_csv_reader(open(filepath)))

        #print self.edges_files
        #print self.nodes_files
        self.nodes.append(None)
        for node_db in self.nodes_db_list:
            self.read_nodes(node_db)
        max_core_node_index = self.node_index
        print max_core_node_index 
        
        # store interactions before storing edges, because we are only going to have interactions for the nodes
        # we collected
        #threads = []
        for interact_name, interact_files in self.interacts_files.iteritems():
            for ireader in interact_files:
                print "Reading interactions for ", interact_name
                #t = threading.Thread(target=self.store_interactions, args=(ireader, self.interact_types_dict[interact_name]))
                #threads.append(t)
                #t.start()
                self.store_interactions(ireader, self.interact_types_dict[interact_name])
        #for t in threads:
        #    t.join()

        for edge_db in self.edges_db_list:
            self.read_friends(edge_db, max_core_node_index)
        print "All nodes and edges read.\nNow reading interactions..."
        return

    def read_nodes(self, nodes_db):
        print "reading nodes"
        counter_node = 0 
        for row in nodes_db:
            username = row[1]
            if self.node_index % 10000 == 0:
                print "Storing information about ", self.node_index
            #if counter_node == self.MAX_NODES_TO_READ:
            #    break
            self.node_index += 1
            counter_node += 1
           
            user_id = self.node_index
            #print temp_key#data_dict
            self.uid_dict[username] = user_id
            if self.node_index <20:
                print username, user_id
            node_data = LastfmDataPreparserCSV.NodeData(original_uid=username, 
                            interaction_types=self.interaction_types)

            new_netnode = self.create_network_node(user_id, 
                            should_have_friends=True, 
                            should_have_interactions=True, node_data=node_data)

            self.nodes.insert(user_id, new_netnode)
        print "All nodes read and stored" 

    def read_friends(self, edge_db, max_core_node_index):
        flist = []
        prev_source_id = None
        #count_nodes_stored = 0
        for source_user, target_user in edge_db:
            #print source_user, target_user
            # assuming edges are always stored in same order as the nodes
            if source_user not in self.uid_dict:
               continue
            if self.uid_dict[source_user] > max_core_node_index:
                continue
            source_id = self.uid_dict[source_user]
            
            if target_user in self.uid_dict:
                friend_id = self.uid_dict[target_user]
            else:
                self.node_index += 1
                friend_id = self.node_index
                self.uid_dict[target_user] = friend_id
                node_data = LastfmDataPreparserCSV.NodeData(original_uid=target_user,
                                interaction_types=self.interaction_types)
                self.nodes.insert(friend_id, self.create_network_node(friend_id,
                                                should_have_friends=False, 
                                                should_have_interactions=False, 
                                                node_data=node_data))
            
            if prev_source_id is not None and prev_source_id != source_id:
                #print prev_source_id, self.MAX_NODES_TO_READ, "Yo"
                self.nodes[prev_source_id].store_friends(flist)
                #count_nodes_stored += 1
                flist = []
                
            flist.append(LastfmDataPreparserCSV.EdgeData(receiver_id=friend_id))
            prev_source_id = source_id
        
        self.nodes[prev_source_id].store_friends(flist)
    
    def store_interactions(self,interact_db, interact_type):
        prev_source_id = None
        ilist = []
        for row in interact_db:
            source_user = row[0]
            if self.use_artists:
                orig_item_id = row[3] # if you want artists
            else:
                orig_item_id = row[2]
            timestamp = int(row[5])
            rating = int(row[6])
            #if source_user not in self.uid_dict:
            #   continue
            source_id = self.uid_dict[source_user]
            
            if prev_source_id is not None and prev_source_id != source_id:
                #print prev_source_id, self.MAX_NODES_TO_READ, "Yo"
                self.nodes[prev_source_id].store_interactions(interact_type, ilist)
                #count_nodes_stored += 1
                ilist = []
                
        
            if orig_item_id in self.itemid_dict:
                item_id = self.itemid_dict[orig_item_id]
            else:
                item_id = self.item_index
                self.itemid_dict[orig_item_id] = item_id
                self.items.insert(self.item_index, (orig_item_id,)) 
                self.item_index += 1
            ilist.append(LastfmDataPreparserCSV.InteractData(item_id=item_id, 
                                    original_item_id=orig_item_id,
                                    timestamp=timestamp, 
                                    rating=rating))
            #print timestamp.encode('utf-8')
            prev_source_id = source_id
        self.nodes[prev_source_id].store_interactions(interact_type, ilist)
        print "Read interactions for ", interact_type
        return


