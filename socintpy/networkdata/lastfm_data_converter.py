from socintpy.store.sqlite_store import SqliteStore
from collections import namedtuple
import logging, sys
import csv
from pprint import pprint

class LastfmDataConverter(object):
    interact_types_dict = {"listen": 0, "love": 1, "ban": 2}
    NodeData = namedtuple('NodeData', 'original_uid interaction_types')
    InteractData = namedtuple('InteractData', 'user_id original_item_id track_url artist_mbid album_mbid timestamp rating')
    EdgeData= namedtuple('EdgeData', 'receiver_id')

    def __init__(self, data_path, cutoff_rating, max_core_nodes):
        self.datadir = data_path
        self.nodes_store_path = data_path + "lastfm_nodes.db"
        self.edges_store_path = data_path + "lastfm_edges.db"
        self.cutoff_rating = None
        self.node_id_map = {} # seed nodes had user_ids, not usernames 
        self.out_nodes = self.datadir + "lastfm_nodes.tsv"
        self.out_interacts = []
        self.iwriters = {}
        for interact_type in LastfmDataConverter.interact_types_dict.keys():
            t = open(self.datadir + "lastfm_" + interact_type + ".tsv", "wb")
            self.out_interacts.append(t)
            self.iwriters[interact_type] = csv.writer(t)
            
        self.out_edges = self.datadir + "lastfm_edges.tsv"

    def __del__(self):
        for f in self.out_interacts:
            f.close()

    def get_all_data(self):
        node_db= SqliteStore(self.nodes_store_path)
        self.read_nodes(node_db)

        edge_db = SqliteStore(self.edges_store_path)
        self.read_friends(edge_db)
        print "Done all"
        print self.node_id_map
        

    def read_nodes(self, nodes_db):
        print "reading nodes"
        nfile=open(self.out_nodes, "wb")
        writer = csv.writer(nfile)
        counter = 1
        for temp_key, data_dict in nodes_db.iteritems():
            orig_user_id = temp_key
            #pprint(data_dict)
            if counter % 1000 == 0:
                print "Storing information about ", counter
                #break
            writer.writerow((temp_key, data_dict['name']))
            self.store_interactions(data_dict['name'], data_dict)
            if orig_user_id != data_dict['name']:
                self.node_id_map[orig_user_id] = data_dict['name']
            counter += 1
        
        nfile.close()    

    def read_friends(self, edge_db):
        efile=open(self.out_edges, "wb")
        ewriter = csv.writer(efile)
        for edge_id, data_dict in edge_db.iteritems(sorted=False):
            if data_dict['source'] not in self.node_id_map:
                source_user = data_dict['source']
            else:
                source_user = self.node_id_map[data_dict['source']]

            if data_dict['target'] in self.node_id_map:
                print "Big mistake", data_dict['target']
                target_user = self.node_id_map[data_dict['target']]
            else:
                target_user = data_dict['target']
            
                
            ewriter.writerow((source_user, target_user))
        
        efile.close()
    
    def store_interactions(self, user_id, data_dict):
        self.store_interactions_by_type(user_id, 'love', data_dict['lovedtracks'])
        self.store_interactions_by_type(user_id, 'ban', data_dict['bannedtracks'])
       
        recent_tracks_list = [] 
        for trackdict in data_dict['tracks']:
            if '@attr' in trackdict:
                #pprint(trackdict)
                if 'nowplaying' in trackdict['@attr']:
                    logging.warn("Now playing track, ignoring %s,%s!" %(data_dict['name'], user_id))
                    continue
            recent_tracks_list.append(trackdict)
        self.store_interactions_by_type(user_id, 'listen', recent_tracks_list)

    def store_interactions_by_type(self,netnode_id, interact_type, interactions_list):
        ilist = []
        for trackdict in interactions_list:
            print trackdict; break
            orig_item_id = trackdict['mbid']
            if 'album' in trackdict:
                album_mbid = trackdict['album']['mbid']
            else:
                album_mbid = ''
            artist_mbid = trackdict['artist']['mbid']
            track_url = trackdict['url']
            if len(orig_item_id) <2 and len(track_url)<2:
                pprint(trackdict)
            ilist.append(LastfmDataConverter.InteractData(user_id=netnode_id, 
                            original_item_id=orig_item_id, track_url=track_url,
                            artist_mbid = artist_mbid, album_mbid = album_mbid,
                            timestamp=trackdict['date']['uts'], rating=1))
            #print "%s" %trackdict['date']['uts']
        
        self.iwriters[interact_type].writerows(ilist)
        return

if __name__ == "__main__":
    part_code = sys.argv[1]
    dir_path = "/mnt/bigdata/lastfm/part" + part_code + "/"
    logging.basicConfig(filename=dir_path+"converter.log", level="DEBUG")
    ldc = LastfmDataConverter(dir_path,None, None)
    ldc.get_all_data()





