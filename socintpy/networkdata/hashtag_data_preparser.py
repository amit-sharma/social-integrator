from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
from pprint import pprint
import os


class HashtagDataPreparser(NetworkDataPreparser):
    def __init__(self, datadir):
        NetworkDataPreparser.__init__(self)
        self.datadir = datadir
        self.itemcounter = 1
        self.allfriends = {}
        self.INTERACTION_TYPES = ["hashtag"]

    def get_all_data(self):
        self.read_nodes()
        self.read_friends()

    def read_nodes(self):
        for root, dirs, files in os.walk(self.datadir):
            for filename in files:
                uid, extn = filename.split(".")
                if extn == "featnames":
                    self._add_to_item_dict(filename)
                if extn == "egofeat":
                    self.nodes[uid] = NetworkNode(uid, {'interactions': self.INTERACTION_TYPES})

        for uid, node in self.nodes.iteritems():
            tagmap = self._get_local_tag_map(uid)
            #pprint(tagmap)
            #pprint(self.items)
            #print uid
            ulikes_file = open(self.datadir + uid + ".egofeat", "r")
            line = ulikes_file.readline()
            like_arr = line.split()
            for i in range(len(like_arr)):
                if like_arr[i].strip("\n") == "1":
                    #print tagmap[i]
                    node.add_interaction("hashtag", self.items[tagmap[i]], None)
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
            if tagname not in self.items:
                self.items[tagname] = self.itemcounter
                self.itemcounter += 1
        return

    def read_friends(self):
        for uid, node in self.nodes.iteritems():
            tagmap = self._get_local_tag_map(uid)
            flikes_file = open(self.datadir + uid + ".feat", "r")
            for line in flikes_file:
                cols = line.split()
                friendid = cols[0]
                node.add_friend(friendid, None)
                if friendid not in self.allfriends:
                    self.allfriends[friendid] = NetworkNode(friendid, {'interactions': self.INTERACTION_TYPES})
                for i in range(1, len(cols)):
                    if cols[i].strip("\n") == "1":
                        itemid = self.items[tagmap[i - 1]]

                        #if itemid not in ucircle.fitems:
                        #    ucircle.fitems[itemid] = set()
                        #ucircle.fitems[itemid].add((friendid, None))

                        #Adding to allfriends too
                        self.allfriends[friendid].add_interaction("hashtag", itemid, None)
            print ("Processed and read", uid)
        return self.nodes, self.allfriends

    def get_artists(self):
        self.artists = {}
        return self.artists


if __name__ == "__main__":
    data = HashtagDataPreparser("/home/asharma/datasets/twitter_jure/")
    data.get_all_data()
    pprint(data.nodes["12831"].node_data)
    pprint(data.nodes["12831"].friends)
    pprint(data.nodes["12831"].interactions)
