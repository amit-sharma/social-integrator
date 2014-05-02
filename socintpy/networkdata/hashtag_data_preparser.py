from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
from pprint import pprint
import os

def handle_os_walk(os_error):
    raise

class HashtagDataPreparser(NetworkDataPreparser):
    def __init__(self, datadir):
        NetworkDataPreparser.__init__(self)
        self.datadir = datadir
        self.itemcounter = 1
        #self.allfriends = {}
        self.interaction_types = ["hashtag"]

    def get_all_data(self):
        self.read_nodes()
        self.read_friends()

    def read_nodes(self):
        for root, dirs, files in os.walk(self.datadir, onerror=handle_os_walk):
            for filename in files:
                uid, extn = filename.split(".")
                if extn == "featnames":
                    self._add_to_item_dict(filename)
                if extn == "egofeat":
                    if uid in self.nodes:
                        self.nodes[uid].mark_as_core()
                    else:
                        self.nodes[uid] = NetworkNode(uid, is_core=True, node_data={'interactions': self.interaction_types})

        for uid, node in self.nodes.iteritems():
            tagmap = self._get_local_tag_map(uid)
            #pprint(tagmap)
            #pprint(self.items)
            #print uid
            ulikes_file = open(self.datadir + uid + ".egofeat", "r")
            line = ulikes_file.readline()
            like_arr = line.split()
            for i in range(len(like_arr)):
                if like_arr[i].strip("\n") == "1" and tagmap[i][0]=='#':
                    #print tagmap[i]
                    node.add_interaction("hashtag", self.items[tagmap[i]], {"hashtag_string":tagmap[i]})
            ulikes_file.close()
        return self.nodes

    def _get_local_tag_map(self, uid):
        """ Function to return a dict that maps hashtag id to a hashtag string.
        """
        featnames_file = open(self.datadir + uid + ".featnames", "r")
        tagmap = {}
        for line in featnames_file:
            cols = line.split()
            hashtag = cols[1].strip("\n")
            num_id = int(cols[0])
            tagmap[num_id] = hashtag
        return tagmap

    def _add_to_item_dict(self, filename):
        """ Populates self.items with unique ids for each hashtag string.
        """
        featname_file = open(self.datadir + filename, "r")
        for line in featname_file:
            cols = line.split()
            tagname = cols[1].strip("\n")
            #print tagname
            if tagname not in self.items and tagname[0] == '#':
                self.items[tagname] = self.itemcounter
                self.itemcounter += 1
        return

    def read_friends(self):
        """ Reads friend data and overwrites previous data (if crawled before) written on the node object for the friend.
        """
        all_core_users =  [(k,v) for k, v in self.nodes.iteritems()]
        for uid, node in all_core_users:
            tagmap = self._get_local_tag_map(uid)
            flikes_file = open(self.datadir + uid + ".feat", "r")
            for line in flikes_file:
                cols = line.split()
                friendid = cols[0]
                if friendid not in self.nodes:
                    self.nodes[friendid] = NetworkNode(friendid, is_core=False, node_data={'interactions': self.interaction_types})

                for i in range(1, len(cols)):
                    if cols[i].strip("\n") == "1" and tagmap[i-1][0]=='#':
                        itemid = self.items[tagmap[i - 1]]
                        self.nodes[friendid].add_interaction("hashtag", itemid, {'hashtag_string':tagmap[i-1]})
                node.add_friend(friendid, self.nodes[friendid], None)
            print ("Processed and read", uid)
        return self.nodes

if __name__ == "__main__":
    data = HashtagDataPreparser("/home/asharma/datasets/twitter_jure/")
    data.get_all_data()
    pprint(data.nodes["12831"].node_data)
    pprint(data.nodes["12831"].friends)
    pprint(data.nodes["12831"].interactions)
