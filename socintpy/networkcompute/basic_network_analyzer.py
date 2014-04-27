from socintpy.networkdata.hashtag_data_preparser import HashtagDataPreparser
from socintpy.util.utils import mean_sd
import random
import heapq

class BasicNetworkAnalyzer(object):
    def __init__(self, networkdata):
        self.netdata = networkdata

    def printAverages(self):
        avg_circle_size = sum([v.getTotalItems() for k,v in data.users.iteritems()]) / len(data.users.keys())
        min_circle_size = min([v.getTotalItems() for k,v in data.users.iteritems()])
        max_circle_size = max([v.getTotalItems() for k,v in data.users.iteritems()])
        print min_circle_size, avg_circle_size, max_circle_size

    def show_basic_stats(self):
        num_core_users = sum([v.is_core for v in self.netdata.nodes.itervalues()])
        print "Number of Core Users", num_core_users
        num_overall_users = len(self.netdata.nodes)
        print "Number of overall users", len(self.netdata.nodes)

        fr_arr = [len(v.friends) for v in self.netdata.nodes.itervalues() if v.is_core]
        print "Mean, SD of number of friends per user", mean_sd(fr_arr)

        items_all=[]
        for v in self.netdata.nodes.itervalues():
            #items_all = items_all.union(v.get_items_interacted_with())
            items_all.extend(v.get_items_interacted_with())
        items_all = set(items_all)
        print "Total number of items", len(items_all)
        print "Types of interactions with items", self.netdata.interaction_types

        items_by_interaction = {}
        for interact_type in self.netdata.interaction_types:
            items_by_interaction[interact_type] = set()
        for v in self.netdata.nodes.itervalues():
            for interact_type, interact_dict in v.interactions.iteritems():
                items_by_interaction[interact_type] |= set(interact_dict.keys())

        for interact_type, items_set in items_by_interaction.iteritems():
            print( "--Total number of items with interaction %s: %d" %(interact_type, len(items_set)) )

        sum_each_interaction_dict = self.get_sum_interactions_by_type()
        for interact_type, total_interacts in sum_each_interaction_dict.iteritems():
            print(interact_type)
            print( "--Total, Mean of %s interactions per user = (%d, %f)"
                   %(interact_type, total_interacts, total_interacts/num_overall_users) )

            print( "--Total, Mean of %s interactions per item = (%d, %f)"
                   %(interact_type, total_interacts, total_interacts/len(items_all)) )

        return

    def compare_circle_global_similarity(self, interact_type, num_random_trials):
        counter = 0
        circle_sims = []
        global_sims = []
        for k,v in self.netdata.get_core_nodes_iterable():
            # First calculating average similarity with random people whose number equals the number of friends a user has.
            avg = 0.0
            #print v.friends
            if len(v.interactions[interact_type]) ==0:
                print "Node has no interactions. Skipping!"
                continue
            for i in range(num_random_trials):
                candidate_users =  set(self.netdata.nodes.keys()) - set(v.friends.keys() + [v.uid])
                rand_people = random.sample(candidate_users, len(v.friends))
                rand_items = self.items_from_people(rand_people, interact_type)
                avg += v.compute_similarity(rand_items, interact_type)
            avg_external_similarity = avg/float(num_random_trials)

            # Second, calculating the similarity of a user with his friends
            friends_list = v.friends.keys()
            circle_items =self.items_from_people(friends_list, interact_type)
            circle_similarity = v.compute_similarity(circle_items, interact_type)
            if circle_similarity is None:
                print "Node has no friends to compute circle similarity. Skipping!"
                continue
            circle_sims.append(circle_similarity)
            global_sims.append(avg_external_similarity)
            counter += 1

        return circle_sims, global_sims

    def items_from_people(self, people_list, interaction_type):
        items_dict={}
        for rp in people_list:
            for item_id, interact_val in self.netdata.nodes[rp].interactions[interaction_type].iteritems():
                if item_id not in items_dict:
                    items_dict[item_id] = set()
                items_dict[item_id].add((rp, None))
        return items_dict

    def get_sum_interactions_by_type(self):
        interactions_per_user = {}
        for interact_type in self.netdata.interaction_types:
            interactions_per_user[interact_type] = 0

        for v in self.netdata.nodes.itervalues():
            for interact_type, interact_dict in v.interactions.iteritems():
                interactions_per_user[interact_type] += len(interact_dict)
        return interactions_per_user

    def compare_circle_global_knnsimilarity(self, interact_type, k):
        sims_global = []
        sims_local = []
        for k,v in self.netdata.get_core_nodes_iterable():
            global_candidates = self.netdata.get_nonfriends_iterable(v)
            sims_global.append(self.compute_knearest_neighbors(v, global_candidates, interact_type, k))
            local_candidates = v.friends.iteritems()
            sims_local.append(self.compute_knearest_neighbors(v, local_candidates, interact_type, k))
            #print sim1, sim2
        return sims_local, sims_global

    def compute_knearest_neighbors(self, node, candidate_nodes_iterable, interact_type, k):
        minheap=[]
        for cnode_id, candidate_node  in candidate_nodes_iterable:
            curr_sim = node.compute_node_similarity(candidate_node.interactions[interact_type], interact_type)
            heapq.heappush(minheap, (curr_sim, candidate_node))
            if len(minheap) > k:
                heapq.heappop(minheap)
        return minheap

    """
    def getItemPopularityInDataset(data):
        likes = {}
        for k, v in data.allusers.iteritems():
            for itemid, created in v.likes:
                if itemid not in likes:
                    likes[itemid] = 0
                likes[itemid] += 1

        likes_hist={}
        in_sum = 0
        for val in likes.values():
            if val<100:
                in_sum += 1
                if val not in likes_hist:
                    likes_hist[val] = 0
                likes_hist[val] += 1
        items_covered_ratio = in_sum /float(len(likes))
        return list(likes_hist.iteritems()), items_covered_ratio, likes.values()
    """

if __name__ == "__main__":
    data = HashtagDataPreparser("/home/asharma/datasets/ttest/")
    data.get_all_data()
    net_analyzer = BasicNetworkAnalyzer(data)
    net_analyzer.show_basic_stats()
