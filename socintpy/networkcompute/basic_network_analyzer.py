#import pyximport; pyximport.install()
from socintpy.cythoncode.cnetwork_node import compute_global_topk_similarity
from socintpy.networkdata.hashtag_data_preparser import HashtagDataPreparser
from socintpy.util.utils import mean_sd
import random
import heapq

from collections import defaultdict

class BasicNetworkAnalyzer(object):
    def __init__(self, networkdata):
        self.netdata = networkdata

    def printAverages(self):
        avg_circle_size = sum([v.getTotalItems() for k,v in data.users.iteritems()]) / len(data.users.keys())
        min_circle_size = min([v.getTotalItems() for k,v in data.users.iteritems()])
        max_circle_size = max([v.getTotalItems() for k,v in data.users.iteritems()])
        print min_circle_size, avg_circle_size, max_circle_size

    def show_basic_stats(self):
        num_interactdata_users = sum([1 for _ in self.netdata.get_nodes_iterable(should_have_interactions=True)])
        print "Number of Users with interaction data", num_interactdata_users
        num_frienddata_users = sum([1 for _ in self.netdata.get_nodes_iterable(should_have_friends=True)])
        print "Number of Users with friendship data", num_frienddata_users

        num_overall_users = self.netdata.get_total_num_nodes()
        print "Number of overall users", num_overall_users

        fr_arr = [v.get_num_friends() for v in self.netdata.get_nodes_iterable(should_have_friends=True)]
        print "Mean, SD of number of friends per user", mean_sd(fr_arr)
        print "Number of users with zero friends", sum([1 for v in fr_arr if v == 0])
        
        items_all=[]
        users_with_zero_interacts = 0
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True):
            #items_all = items_all.union(v.get_items_interacted_with())
            items_list = v.get_all_items_interacted_with()
            if len(items_list)==0:
                users_with_zero_interacts += 1
            items_all.extend(items_list)
        items_all = set(items_all)
        print "Total number of items", len(items_all)
        print "Types of interactions with items", self.netdata.interaction_types
        print "Total number of users with zero interacts (across all types)", users_with_zero_interacts
        
        items_by_interaction = []
        for interact_type in self.netdata.interaction_types:
            items_by_interaction.append(set())

        for v in self.netdata.get_nodes_iterable(should_have_interactions=True):
            for interact_type in self.netdata.interaction_types:
                items_by_interaction[interact_type] |= set(v.get_items_interacted_with(interact_type))

        for i in range(len(items_by_interaction)):
            print( "--Total number of items with interaction %s: %d" %(i, len(items_by_interaction[i])) )
        
        sum_each_interaction_dict = self.get_sum_interactions_by_type()
        for interact_type, total_interacts in sum_each_interaction_dict.iteritems():
            #print(interact_type)
            print( "--Total, Mean, Max, Min of %d-type interactions per user = (%d, %f)"
                   %(interact_type, total_interacts, total_interacts/float(num_interactdata_users)))

            print( "--Total, Mean, Max, Min of %d-type interactions per item = (%d, %f)"
                   %(interact_type, total_interacts, total_interacts/float(len(items_all))) )
         
        return


    def get_items_popularity(self, interact_type, rating_cutoff):
        items_pop =  defaultdict(int)
        for node in self.netdata.get_nodes_iterable(should_have_interactions=True):
            interacts = node.get_items_interacted_with(interact_type, rating_cutoff)
            for item_id in interacts:
                items_pop[item_id] += 1

        return items_pop

    def get_user_interacts(self, interact_type, rating_cutoff):
        #user_interacts = []
        for node in self.netdata.get_nodes_iterable(should_have_interactions=True):
            interacts = node.get_items_interacted_with(interact_type, rating_cutoff, return_timestamp=True)
            for item_id, timestamp in interacts:
                #user_interacts.append((node.uid, item_id, timestamp))
                yield (node.uid, item_id, timestamp)
        #return user_interacts

    def get_user_friends(self):
        #friends = []
        for node in self.netdata.get_nodes_iterable(should_have_friends=True):
            friends = node.get_friend_ids()
            for friend_id in friends:
                #friends.append((node.uid, friend_id))
                yield (node.uid, friend_id)
        #return friends
     
    def get_high_freq_items(self, items_set, min_freq, return_duplicates):
        freq_items = {}
        for item in items_set:
            item_id = item[0] if return_duplicates else item
            if item_id not in freq_items:
                freq_items[item_id] = 0
            freq_items[item_id] += 1
        return [key for key, value in freq_items.iteritems() if value >=min_freq]
    
    def compare_interaction_types(self, min_exposure=1, return_duplicates=True):
        num_interacts = {}
        for key in self.netdata.interact_types_dict.keys():
            num_interacts[key] = []
        for node in self.netdata.get_nodes_iterable(should_have_interactions=True):
            for key, interact_type in self.netdata.interact_types_dict.iteritems():
                items = node.get_items_interacted_with(interact_type, 
                                                       return_timestamp=return_duplicates)
                if key=="listen":
                    num_items = len(self.get_high_freq_items(items, min_exposure,
                                                             return_duplicates))
                else:
                    num_items = len(items)
                num_interacts[key].append(num_items)
#for arr in num_interacts.itervalues():
#            arr.sort()
        return num_interacts

    def compare_circle_global_similarity(self, interact_type, num_random_trials, cutoff_rating):
        counter = 0
        circle_sims = []
        global_sims = []
        all_node_ids = range(1, self.netdata.get_total_num_nodes()+1)
        for v in self.netdata.get_nodes_iterable(should_have_friends=True, should_have_interactions=True):
            # First calculating average similarity with random people whose number equals the number of friends a user has.
            if counter % 1000 == 0:
                print "Starting node", counter
            counter += 1
            avg = 0.0
            #print v.friends
            if not v.has_interactions(interact_type) or not v.has_friends():
                #print "Node has no interactions. Skipping!"
                continue
            
            count_sim_trials = 0
            #candidate_users = set(self.netdata.get_node_ids()) - set(v.get_friend_ids() + [v.uid])
            #candidate_users = [True]*(self.netdata.get_total_num_nodes()+1)
            #for fid in v.get_friend_ids():
            #    candidate_users[fid] = False
            #candidate_users[v.uid] = False
            friends_list =  v.get_friend_ids()
            num_friends = v.get_num_friends()
            for i in range(num_random_trials):
                #print all_node_ids, num_friends*2+1
                rand_people = random.sample(all_node_ids, num_friends*2+1)
                rand_people_iter = self.set_difference_generator(rand_people, friends_list+[v.uid], num_friends)
                    

                #rand_items = self.items_interacted_by_people(rand_people_iter, interact_type, rating_cutoff=cutoff_rating)
                #trial_global_similarity=v.compute_similarity(rand_items, interact_type, cutoff_rating)
                trial_global_similarity = v.compute_mean_similarity(self.netdata.get_node_objs(rand_people_iter), interact_type, cutoff_rating)
                #trial_global_similarity=1
                if trial_global_similarity is not None:
                    avg += trial_global_similarity
                    count_sim_trials += 1
            if count_sim_trials == 0:
                print "Node has no global sim", v.uid, v.get_num_friends()
                continue
            avg_external_similarity = avg/float(count_sim_trials)

            # Second, calculating the similarity of a user with his friends
            #friends_list = v.get_friend_ids()
            #circle_items = self.items_interacted_by_people(friends_list, interact_type, rating_cutoff=cutoff_rating)
            #circle_similarity = v.compute_similarity(circle_items, interact_type, cutoff_rating)
            avg_circle_similarity = v.compute_mean_similarity(self.netdata.get_friends_nodes(v), interact_type, cutoff_rating)
            #circle_similarity=1
            if avg_circle_similarity is None:
                print "Node has no friends to compute circle similarity. Skipping!"
                continue
            circle_sims.append((v.uid, avg_circle_similarity))
            global_sims.append((v.uid, avg_external_similarity))

        return circle_sims, global_sims

    def set_difference_generator(self, list1, list2, max_results):
        counter = 0
        for val in list1:
            if counter == max_results:
                break
            if val not in list2:
                counter += 1 
                yield val

    def items_interacted_by_people(self, people_list, interaction_type, rating_cutoff=None):
        items_dict={}
        for node_id in people_list:
            item_ids = self.netdata.nodes[node_id].get_items_interacted_with(interaction_type, rating_cutoff)
            for itemid in item_ids:
                if itemid not in items_dict:
                    items_dict[itemid] = []
                items_dict[itemid].append((node_id, None))
        return items_dict

    def get_sum_interactions_by_type(self):
        interactions_per_user = {}
        for interact_type in self.netdata.interaction_types:
            interactions_per_user[interact_type] = 0

        for v in self.netdata.get_nodes_iterable(should_have_interactions=True):
            for interact_type in self.netdata.interaction_types:
                items = v.get_items_interacted_with(interact_type, 
                                                       return_timestamp=True)
                interactions_per_user[interact_type] += len(items)
        return interactions_per_user

    #TODO have pearson correlation instead of jaccard because these ratings are real valued. going with a cutoff now.
    def compare_circle_global_knnsimilarity(self, interact_type, klim, cutoff_rating):
        sims_local = []
        sims_global = []
        counter = 0
        #cutoff_rating = 3
        #mat=compute_global_topk_similarity(self.netdata.get_all_nodes(), interact_type, klim)
        for v in self.netdata.get_nodes_iterable(should_have_friends=True, should_have_interactions=True):
            if counter % 10 == 0:
                print "Starting node", counter
            counter += 1
            if v.has_interactions(interact_type):

                np_array = v.compute_local_topk_similarity(self.netdata.get_friends_nodes(v), interact_type, klim, cutoff_rating)
                if np_array is not None and len(np_array) > 0:
                    local_sim_avg = sum(np_array)/len(np_array)
                else:
                    #print "Error in finding local %d neighbors for %s" % (klim,v.uid), localk_neighbors
                    continue
                """
                local_candidates = self.netdata.get_friends_iterable(v)
                localk_neighbors = self.compute_knearest_neighbors(v, local_candidates, interact_type, klim)
                if len(localk_neighbors) == klim:
                    localsim_vector = [sim for sim, neigh in localk_neighbors]
                    local_sim_avg = sum(localsim_vector)/len(localsim_vector)
                else:
                    #print "Error in finding local %d neighbors for %s" % (klim,v.uid), localk_neighbors
                    continue
                """
                np_array = v.compute_global_topk_similarity(self.netdata.get_all_nodes(), interact_type, klim, cutoff_rating)
                #np_array = v.compute_global_topk_similarity_mat(mat, klim)
                #print np_array
                global_sim_avg = sum(np_array)/len(np_array)
                """
                global_candidates = self.netdata.get_nonfriends_iterable(v)
                globalk_neighbors = self.compute_knearest_neighbors(v, global_candidates, interact_type, klim)

                if len(globalk_neighbors) == klim:
                    global_simvector = [sim for sim, neigh in globalk_neighbors]
                    global_sim_avg = sum(global_simvector)/len(global_simvector)
                else:
                    print "Error in finding global %d neighbors" % klim,"for",v.uid
                    continue
                """
                sims_global.append((v.uid, global_sim_avg))

                sims_local.append((v.uid, local_sim_avg))
                #print(global_sim_avg, local_sim_avg)

            else:
                print "Error in finding %d neighbors for %s:(Has no interactions)" % (klim,v.uid)
            #print sim1, sim2
        return sims_local, sims_global


    @staticmethod
    def compute_knearest_neighbors(node, candidate_nodes_iterable, interact_type, k, data_type="all" ):
        minheap=[]
        data_type_code = ord(data_type[0])
        for candidate_node in candidate_nodes_iterable:
            curr_sim = node.compute_node_similarity(candidate_node, interact_type, data_type_code)
            #print "Current sim at basic is", curr_sim
            #curr_sim = 1
            if curr_sim is not None:
                heapq.heappush(minheap, (curr_sim, candidate_node))
                if len(minheap) > k:
                    heapq.heappop(minheap)
        return minheap

    
    def get_node_details(self, node_id):
        node = self.netdata.nodes[node_id] 
        node.get_details(self.netdata.interaction_types[0])
        return

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
