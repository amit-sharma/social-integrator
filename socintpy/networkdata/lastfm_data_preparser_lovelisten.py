from socintpy.networkdata.lastfm_data_preparser_simple import LastfmDataPreparserSimple
from socintpy.networkdata.network_node import NetworkNode
from socintpy.store.sqlite_store import SqliteStore
from socintpy.util.unicode_csv import unicode_csv_reader
from collections import namedtuple
import logging, os, codecs, sys, csv
import threading
from multiprocessing import Pool
import time, datetime

LOCAL_SPLITT_TIMESTAMP = int(time.mktime(datetime.datetime.strptime("2013/07/01", "%Y/%m/%d").timetuple()))

def handle_os_walk(os_error):
    raise

# comparing numbers that do not have same no. of digits 
def compare_filenames(x, y):
    if len(x) > len(y):
        return 1
    elif len(x) < len(y):
        return -1
    elif x > y:
        return 1
    else:
        return -1

class LastfmDataPreparserLovelisten(LastfmDataPreparserSimple):
    #interact_types_dict = {"listen": 0, "love": 1, "ban": 2}
    #NodeData = namedtuple('NodeData', 'original_uid interaction_types')
    #InteractData = namedtuple('InteractData', 'item_id original_item_id timestamp rating')
    #EdgeData= namedtuple('EdgeData', 'receiver_id')
    #ItemData = namedtuple('ItemData', 'item_id') # not used currently
    """
    def __init__(self, data_path, node_impl, cutoff_rating, max_core_nodes, 
                 store_dataset, use_artists, interact_type_val, min_interactions_per_user):
        NetworkDataPreparser.__init__(self, node_impl, data_path,
                                      max_core_nodes=max_core_nodes, 
                                      store_dataset=store_dataset,
                                      min_interactions_per_user=min_interactions_per_user)
        self.datadir = data_path
        self.nodes_db_list = []
        self.edges_db_list = []
        self.interaction_types = sorted(LastfmDataPreparserSimple.interact_types_dict.values())
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
        self.interact_type_val = interact_type_val
        #globals().update(self.named_tuple_dict)
    
    def get_all_data(self):
        encoding = "utf-8"
        print "Starting to load data from ", self.datadir
        print "Loading data only for interaction", self.interact_type_val
        print "Using artists:", self.use_artists
        print "Using cutoffrating", self.cutoff_rating
        print "Using min_interactions_per_user", self.min_interactions_per_user

        print "Input files:"
        for root, dirs, files in os.walk(self.datadir, onerror=handle_os_walk):
            sorted_files = sorted(files, cmp=compare_filenames)
            for filename in sorted_files:
                fname_chunks = filename.split(".")
                uname = fname_chunks[0]
                uname_chunks = uname.split("_")
                filepath = os.path.join(root, ".".join(fname_chunks))
                if uname_chunks[0]=="egodata" and filename.endswith(".tsv"):# and len(uname_chunks[1])<=5:
                    self.nodes_files.append(filepath)
                    nodes_db_obj = codecs.open(filepath, encoding=encoding)
                    self.nodes_db_list.append(nodes_db_obj)
                    sys.stdout.write(filename+",")
        print " "
        self.nodes.append(None)
        map(self.read_nodes, self.nodes_files)
        print "filtering data based on min_interactions_per_user=", self.min_interactions_per_user
        print "Number of nodes unfiltered is, like,", len(self.nodes)-1
        self.filter_min_interaction_nodes(self.interact_type_val, self.min_interactions_per_user)
        items_interacted_with_dict = self.compute_store_total_num_items()
        self.total_num_items = len(items_interacted_with_dict)

        # Not reading all_item_ids file for now.
        #self.item_ids_list = self.read_all_item_ids(self.datadir+"itemmap.tsv") 
        self.print_summary()
        return
    """
    def read_nodes(self, node_file):
        nodes_db = codecs.open(node_file, encoding='utf-8')
        print "reading nodes", node_file
        nodes_db.readline() # first line is a comment
        counter_node = 1 
        while True:
            line = nodes_db.readline()
            if not line: break
            row = line.strip("\r\n ")
            cols = row.split(' ')
            #print cols[0], cols[1]
            user_id = int(cols[0])
            should_have_friends = bool(int(cols[1]))
#           sys.exit(1)
            node_data = LastfmDataPreparserSimple.NodeData(original_uid=None, interaction_types=self.interaction_types)
            new_netnode = None 
            new_netnode = self.create_network_node(user_id, 
                            should_have_friends=should_have_friends, 
                            should_have_interactions=True, node_data=node_data)
           
            fr_line = nodes_db.readline()
            if new_netnode is not None:
                row = fr_line.strip("\n\r ").split(' ')
                if row[0]!="None":
                    fids = [int(val) for val in row]
                    new_netnode.store_friend_ids(fids)

            # CAUTION: only reading one of the interact types!!
            interact_tuples = []
            for interact_type in self.interaction_types:
                line= nodes_db.readline()
                if interact_type==1:
                    row = line.strip("\n\r ").split(' ')
                    if row[0]!="None":
                        #if len(row) >= self.min_interactions_per_user:
                        cells = [val.split(':') for val in row]
                        interact_tuples.extend([LastfmDataPreparserSimple.InteractData(int(item_id), None, int(timestamp), 1) for item_id, timestamp in cells])
                        new_netnode.store_interactions(interact_type, interact_tuples, do_sort=True)
                if interact_type==0:
                    row = line.strip("\n\r ").split(' ')
                    if row[0]!="None":
                        if len(row) >= self.min_interactions_per_user/2:
                            cells = [val.split(':') for val in row]
                            listen_tuples =[LastfmDataPreparserSimple.InteractData(int(item_id), None, int(timestamp), 1) for item_id, timestamp in cells if timestamp > LOCAL_SPLITT_TIMESTAMP]
                            if len(listen_tuples) >= self.min_interactions_per_user/2:
                                new_netnode.store_listens_special(0, listen_tuples, do_sort=True)
        
                
            if counter_node % 10000 == 0:
                print "Stored information about ", counter_node
            #if counter_node == self.MAX_NODES_TO_READ:
            #    break
            counter_node += 1
           
            self.nodes.insert(user_id, new_netnode)
        nodes_db.close()
    """
    def read_all_item_ids(self, itemmap_file):
        itemmap = codecs.open(itemmap_file, encoding='utf-8')
        item_ids_list = []
        for line in itemmap:
            cols = line.strip("\n\r ").split(' ')
            item_id = int(cols[1])
            item_ids_list.append(item_id)
        return item_ids_list
    """
