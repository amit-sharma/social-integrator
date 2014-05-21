from collections import namedtuple
from socintpy.networkdata.network_data_preparser import NetworkDataPreparser
from socintpy.networkdata.network_node import NetworkNode
from pprint import pprint
import os

def handle_os_walk(os_error):
    raise

class HashtagDataPreparser(NetworkDataPreparser):
    interaction_types = range(1)
    HASHTAG = interaction_types[0]
    NodeData = namedtuple('NodeData', 'original_node_id interaction_types')
    InteractData = namedtuple('InteractData', 'item_id timestamp rating')
    EdgeData = namedtuple('EdgeData', 'receiver_id')
    def __init__(self, datadir, node_impl):
        NetworkDataPreparser.__init__(self, node_impl)
        self.datadir = datadir
        self.item_counter = 1
        self.node_index = 1
        self.item_index = 1
        self.nodes.insert(0, None)
        self.items.insert(0, None)
        self.items_dict = {}
        #self.allfriends = {}
        #self.interaction_types = ["hashtag"]

    def get_all_data(self):
        node_id_dict = self.read_nodes()
        self.read_friends(node_id_dict)

    def read_nodes(self):
        for root, dirs, files in os.walk(self.datadir, onerror=handle_os_walk):
            for filename in files:
                uname, extn = filename.split(".")
                if extn == "featnames":
                    self._add_to_item_dict(filename)

        node_id_dict={}
        for _, _, files in os.walk(self.datadir, onerror=handle_os_walk):
            for filename in files:
                uname, extn = filename.split(".")
                if extn == "egofeat":
                    node_data = HashtagDataPreparser.NodeData(uname, HashtagDataPreparser.interaction_types)
                    if self.node_index in self.nodes:
                        self.nodes[self.node_index].mark_as_core()
                        print "Oh, we should not come here", uname
                    else:


                        #for node_obj in self.nodes:
                        tagmap = self._get_local_tag_map(uname)
                        #pprint(tagmap)
                        #pprint(self.items)
                        #print uid
                        ulikes_file = open(self.datadir + uname + ".egofeat", "r")
                        line = ulikes_file.readline()
                        like_arr = line.split()
                        idata_list = []
                        for i in range(len(like_arr)):
                            if like_arr[i].strip("\n") == "1":# and tagmap[i][0] == '#':
                                #print tagmap[i]
                                # TODO do something about original/unique hashtag strings
                                idata_list.append(HashtagDataPreparser.InteractData(self.items_dict[tagmap[i]], timestamp="", rating=1))
                        ulikes_file.close()
                        if len(idata_list) >= self.min_interactions:  # adding only core users with at least one hashtag interaction
                            newnode = self.create_network_node(self.node_index, should_have_friends=True,
                                                           should_have_interactions=True,
                                                           node_data=node_data)
                            newnode.store_interactions(HashtagDataPreparser.HASHTAG, idata_list)

                            self.nodes.insert(self.node_index, newnode)
                            node_id_dict[uname] = self.node_index
                            self.node_index += 1

        return node_id_dict

    def _get_local_tag_map(self, uname):
        """ Function to return a dict that maps hashtag id to a hashtag string.
        """
        featnames_file = open(self.datadir + uname + ".featnames", "r")
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
            if tagname not in self.items_dict:# and tagname[0] == '#':
                self.items_dict[tagname] = self.item_counter
                self.item_counter += 1
        return

    def read_friends(self, node_id_dict):
        """ Reads friend data and does not overwrites previous data of hashtags (if crawled before) written on the node object for the friend.
        """
        #all_core_users =  [(k,v) for k, v in self.nodes.iteritems()]
        core_nodes_arr = []
        for uname, node_id in node_id_dict.iteritems():
            core_nodes_arr.append((uname, node_id))

        loop_counter = 0
        for uname, node_id in core_nodes_arr:
            tagmap = self._get_local_tag_map(uname)
            flikes_file = open(self.datadir + uname + ".feat", "r")
            friends_list = []
            for line in flikes_file:
                cols = line.split()
                friendname = cols[0]
                curr_friend_id = None
                if friendname not in node_id_dict:
                    node_data = HashtagDataPreparser.NodeData(friendname, HashtagDataPreparser.interaction_types)
                    self.nodes.insert(self.node_index, self.create_network_node(self.node_index, should_have_friends=False,
                                                                           should_have_interactions=True,
                                                                           node_data=node_data))
                    node_id_dict[friendname] = self.node_index
                    self.node_index += 1

                    curr_friend_id = node_id_dict[friendname]
                    #print curr_friend_id, len(self.nodes)
                    idata_list = []

                    for i in range(1, len(cols)):
                        if cols[i].strip("\n") == "1" and tagmap[i-1][0]=='#':
                            itemid = self.items_dict[tagmap[i - 1]]
                            idata = HashtagDataPreparser.InteractData(itemid, "", 1)
                            idata_list.append(idata)
                    self.nodes[curr_friend_id].store_interactions(HashtagDataPreparser.HASHTAG, idata_list)
                else:
                    curr_friend_id = node_id_dict[friendname]
                fdata = HashtagDataPreparser.EdgeData(curr_friend_id)
                friends_list.append(fdata)
            self.nodes[node_id].store_friends(friends_list)
            if loop_counter % 100 == 0:
                print ("Processed and read", loop_counter)
            loop_counter += 1
        return self.nodes

if __name__ == "__main__":
    data = HashtagDataPreparser("/home/asharma/datasets/twitter_jure/")
    data.get_all_data()
    pprint(data.nodes["12831"].node_data)
    pprint(data.nodes["12831"].friends)
    pprint(data.nodes["12831"].interactions)
