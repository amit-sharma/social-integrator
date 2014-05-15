import pyximport; pyximport.install()
from socintpy.cythoncode.cnetwork_node import compute_global_topk_similarity
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
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True):
            #items_all = items_all.union(v.get_items_interacted_with())
            items_all.extend(v.get_all_items_interacted_with())
        items_all = set(items_all)
        print "Total number of items", len(items_all)
        print "Types of interactions with items", self.netdata.interaction_types
        
        items_by_interaction = []
        for interact_type in self.netdata.interaction_types:
            items_by_interaction.append(set())

        for v in self.netdata.get_nodes_iterable(should_have_interactions=True):
            for interact_type in self.netdata.interaction_types:
                items_by_interaction[interact_type] |= set(v.get_items_interacted_with(interact_type))

        for i in xrange(len(items_by_interaction)):
            print( "--Total number of items with interaction %s: %d" %(i, len(items_by_interaction[i])) )
        
        sum_each_interaction_dict = self.get_sum_interactions_by_type()
        for interact_type, total_interacts in sum_each_interaction_dict.iteritems():
            #print(interact_type)
            print( "--Total, Mean of %d interactions per user = (%d, %f)"
                   %(interact_type, total_interacts, total_interacts/float(num_interactdata_users)) )

            print( "--Total, Mean of %d interactions per item = (%d, %f)"
                   %(interact_type, total_interacts, total_interacts/float(len(items_all))) )
         
        return

    def compare_circle_global_similarity(self, interact_type, num_random_trials):
        counter = 0
        circle_sims = []
        global_sims = []
        all_node_ids = xrange(1, self.netdata.get_total_num_nodes()+1)
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
                rand_people = random.sample(all_node_ids, num_friends*2+1)
                rand_people_iter = self.set_difference_generator(rand_people, friends_list+[v.uid], num_friends)
                    
                    #TODO implement filter for non-friends
                
                rand_items = self.items_interacted_by_people(rand_people_iter, interact_type)
                trial_global_similarity=v.compute_similarity(rand_items, interact_type)
                #trial_global_similarity=1
                if trial_global_similarity is not None:
                    avg += trial_global_similarity
                    count_sim_trials += 1
            if count_sim_trials == 0:
                print "Node has no global sim", v.get_num_friends()
                continue
            avg_external_similarity = avg/float(count_sim_trials)

            # Second, calculating the similarity of a user with his friends
            friends_list = v.get_friend_ids()
            circle_items = self.items_interacted_by_people(friends_list, interact_type)
            circle_similarity = v.compute_similarity(circle_items, interact_type)
            #circle_similarity=1
            if circle_similarity is None:
                print "Node has no friends to compute circle similarity. Skipping!"
                continue
            circle_sims.append(circle_similarity)
            global_sims.append(avg_external_similarity)

        return circle_sims, global_sims

    def set_difference_generator(self, list1, list2, max_results):
        counter = 0
        for val in list1:
            if counter == max_results:
                break
            if val not in list2:
                counter += 1 
                yield val

    def items_interacted_by_people(self, people_list, interaction_type):
        items_dict={}
        for node_id in people_list:
            item_ids = self.netdata.nodes[node_id].get_items_interacted_with(interaction_type)
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
                interactions_per_user[interact_type] += len(v.get_items_interacted_with(interact_type))
        return interactions_per_user

    def compare_circle_global_knnsimilarity(self, interact_type, klim):
        sims_local = []
        sims_global = []
        counter = 0
        #mat=compute_global_topk_similarity(self.netdata.get_all_nodes(), interact_type, klim)
        for v in self.netdata.get_nodes_iterable(should_have_friends=True, should_have_interactions=True):
            if counter % 1000 == 0:
                print "Starting node", counter
            counter += 1
            if v.has_interactions(interact_type):

                np_array = v.compute_local_topk_similarity(self.netdata.get_friends_nodes(v), interact_type, klim)

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
                np_array = v.compute_global_topk_similarity(self.netdata.get_all_nodes(), interact_type, klim)
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
                sims_global.append(global_sim_avg)

                sims_local.append(local_sim_avg)
                #print(global_sim_avg, local_sim_avg)

            else:
                print "Error in finding %d neighbors for %s:(Has no interactions)" % (klim,v.uid)
            #print sim1, sim2
        return sims_local, sims_global

    @staticmethod
    def compute_knearest_neighbors(node, candidate_nodes_iterable, interact_type, k, data_type="all"):
        minheap=[]
        data_type_code = ord(data_type[0])
        for candidate_node in candidate_nodes_iterable:
            curr_sim = node.compute_node_similarity(candidate_node, interact_type, data_type_code)

            #curr_sim = 1
            if curr_sim is not None:
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
