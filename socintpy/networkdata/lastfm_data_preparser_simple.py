from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
from socintpy.store.sqlite_store import SqliteStore
from socintpy.util.unicode_csv import unicode_csv_reader
from collections import namedtuple
import logging, os, codecs, sys, csv
import threading

def handle_os_walk(os_error):
    raise

class LastfmDataPreparserSimple(NetworkDataPreparser):
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
        
        self.cutoff_rating = None
       
        self.nodes_files = []
        self.edges_files = []
        self.interacts_files = {}
        for interact_name in self.interact_types_dict.keys():
            self.interacts_files[interact_name] = []
        self.use_artists = use_artists

    def get_all_data(self):
        encoding = "utf-8"
        for root, dirs, files in os.walk(self.datadir, onerror=handle_os_walk):
            for filename in files:
                fname_chunks = filename.split(".")
                print filename
                uname = fname_chunks[0]
                uname_firstchunk = uname.split("_")[0]
                filepath = os.path.join(root, ".".join(fname_chunks))
                if uname_firstchunk=="egodata" and filename.endswith(".tsv"):
                    self.nodes_files.append(filepath)
                    nodes_db_obj = codecs.open(filepath, encoding=encoding)
                    self.nodes_db_list.append(nodes_db_obj)

        self.nodes.append(None)
        for node_db in self.nodes_db_list:
            self.read_nodes(node_db)
        
        return

    def read_nodes(self, nodes_db):
        print "reading nodes"
        counter_node = 1 
        nodes_db.readline() # first line is a comment
        while True:
            line = nodes_db.readline()
            if not line: break
            row = line.strip("\r\n ").split(',')
            user_id, should_have_friends = row.split(' ')
#print username
#            sys.exit(1)
            line = nodes_db.readline()
            row = line.strip("\n\r ").split(' ')
            fids = [int(val) for val in row]

            for key, value in self.interact_types_dict.iteritems():
                line= nodes_db.readline()
                row = line.strip("\n\r ").split(' ')
                cells = [val.split(':') for val in row]
                interacts = [(int(item_id), int(timestamp), 1) for item_id, timestamp in cells]
            if counter_node % 10000 == 0:
                print "Storing information about ", counter_node
            #if counter_node == self.MAX_NODES_TO_READ:
            #    break
            counter_node += 1
           
            node_data = LastfmDataPreparserSimple.NodeData(original_uid=None, interaction_types=self.interaction_types)

            new_netnode = self.create_network_node(user_id, 
                            should_have_friends=should_have_friends, 
                            should_have_interactions=True, node_data=node_data)
            new_netnode.store_friend_ids(fids)
            for interact_type in sorted(self.interact_types_dict.values()):
                new_netnode.store_interaction_ids(interacts, interact_type)
            self.nodes.insert(user_id, new_netnode)
        nodes_db.close()
        print "All nodes read and stored" 

