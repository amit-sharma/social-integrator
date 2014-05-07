import pyximport; pyximport.install()
import socintpy.cythoncode.cnetwork_node as cnetwork_node
from collections import namedtuple
import heapq
import operator
import math, random
import socintpy.util.utils as utils


class NetworkNode(cnetwork_node.CNetworkNode):
    FRIEND_ONLY = 2
    CORE_USER = 1
    user_sim = {}
    __slots__ = ('node_data', 'interaction_types', 'friends', 'is_core')
    #__slots__ = 'uid', 'has_friends', 'has_interactions','node_data', 'interactions', 'interaction_types', 'friends', 'is_core')

    def __init__(self, uid, has_friends=True, has_interactions=True, node_data=None):
        #self.uid = uid
        #self.has_friends = has_friends
        #self.has_interactions = has_interactions
        super(NetworkNode, self).__init__(uid, has_friends, has_interactions, node_data)
        self.node_data = node_data
        #print getattr(self, 'uid')
        #if self.has_interactions:
        #    self.interactions = []
        #    self.interaction_types = []
        #    if node_data is not None and node_data.interaction_types is not None:
        #        self.interaction_types = node_data.interaction_types
        #        self.register_interactions(self.interaction_types)

        if self.has_friends:
            self.friends = []


    def register_interactions(self, interactions_list):
        for interact_type in interactions_list:
            self.interactions.append([])

    def mark_as_core(self):
        self.is_core = True

    def add_interaction(self, interaction_name, item_id, interaction_data):
        self.interactions[interaction_name].append((item_id, interaction_data))

    def add_friend(self, friendid, friend_node,  friendship_data):
        self.friends.append((friendid,friend_node, friendship_data))

    def get_items_interacted_with(self):
        interacted_items = set()
        for interact_dict in self.interactions.itervalues():
            interacted_items = interacted_items.union(interact_dict.keys())
        return interacted_items

    def get_friendnodes_iterable(self):
        for k, v_dict in self.friends.iteritems():
            yield k, v_dict['friend_node']
    """
    def createLikesOnlySet(self):
        self.likes_only = set()
        for k, created in self.likes:
            self.likes_only.add(k)
        return self.likes_only

    def getTotalItems(self):
        length_fitems = sum([len(v) for k,v in self.fitems.iteritems()])
        return len(self.likes) + length_fitems
    """
    def compute_friends_simiilarity(self, interact_type):
        return self.externalSimilarity(self.get_friends_interactions(interact_type))

    def compute_nonweighted_friends_similarity(self, interact_type):
        return self.externalNonWeightedSimilarity(self.get_friends_interactions(interact_type))

    def compute_similarity(self, items, interact_type):
        if len(self.interactions[interact_type]) == 0 or len(items) == 0:
            return None
        simscore=float(0)
        for item_id, interact_data in self.interactions[interact_type].iteritems():
            if item_id in items:
                simscore += len(items[item_id])
        #return simscore/(len(self.interactions[interact_type])*len(self.friends))
        return simscore/ (utils.l2_norm(self.interactions[interact_type],binary=True)* utils.l2_norm(items, binary=False))

    def externalNonWeightedSimilarity(self, items):
        simscore=float(0)
        for k,created_time in self.likes:
            if k in items:
                simscore += 1
        return simscore/(len(self.likes))

    """

    def getNumFriends(self):
        return len(self.friends)

    def getNumLikes(self):
        return len(self.likes)

    """

    def getAllCircleItems(self):
        circle_items = []
        circle_items.extend([v1 for v1, v2 in self.interactions])
        items.extend(self.fitems.keys())
        return set(items)
    """
    def getOverallCircleLikes(self, itemid):
        numlikes = 0
        for k,v in self.likes:
            if k==itemid:
                numlikes += 1
                break
        if itemid in self.fitems:
            numlikes += len(self.fitems[itemid])
        return numlikes

    def isMale(self):
        if self.gender == "male":
            return True
        else:
            return False

    def mergeLikes(self, other_likes):
        #test code
        for itemid, created in self.likes:
            if itemid in [v1 for v1,v2 in other_likes]:
                if (itemid,created) not in other_likes:
                    print itemid, created, other_likes

        #merge code
        self.likes.update(other_likes)
        return self.likes

    def calcCircleSparsity(self):
        num_circle_items = len(self.getAllCircleItems())
        num_circle_likes = self.getTotalItems()
        num_people = self.getNumFriends() + 1
        sparsity = num_circle_likes / (float(num_circle_items*num_people))
        return sparsity
    """
    def compute_node_similarity(self, others_interactions, interact_type, my_interactions=None):
        if my_interactions is None:
            my_interactions = self.interactions[interact_type]

        if len(my_interactions) == 0 or len(others_interactions) == 0:
            return None
        sim_score = 0
        for item_id in my_interactions.iterkeys():
            if item_id in others_interactions:
                sim_score += 1
        sim_score = sim_score/ (utils.l2_norm(my_interactions, binary=True) * utils.l2_norm(others_interactions, binary=True))
        return sim_score

        #simscore = len(my_interactions & others_interactions)
        #unionsum = len(my_interactions) + len(others_interactions) - simscore
        #usersim = simscore/float(unionsum)

        #if (self.uid, other_uid) in UCircle.user_sim:
        #    print UCircle.user_sim[(self.uid, other_uid)], usersim, self.uid, other_uid
        #    pprint(UCircle.user_sim)
        #UCircle.user_sim[(self.uid, other_uid)] = usersim

    def circleKNearestSim(self, allfr, num, my_likes):
        minheap=[]
        for frid in self.friends:
            curr_sim = self._getUserSimilarity(my_likes, allfr[frid].likes_only, frid)
            heapq.heappush(minheap, (curr_sim, allfr[frid]))
            if len(minheap) > num:
                heapq.heappop(minheap)
        return minheap
    """
    def circleRandomUsers(self, allfr, num):
        selected_friends = random.sample(self.friends, min(num, len(self.friends)))
        res = []
        for frid in selected_friends:
            res.append((1, allfr[frid]))
        return res

    def calcCircleKNearestSim(self, allfr, num, my_likes=None):
        if my_likes is None:
            my_likes = self.likes_only
        minheap = self.circleKNearestSim(allfr, num, my_likes)
        return sum([self._getUserSimilarity(my_likes, uc[1].likes_only, uc[1].uid) for uc in minheap])/float(len(minheap))

    def globalKNearestSim(self, allfr, num, my_likes):
        minheap=[]
        for k,v in allfr.iteritems():
            if k not in self.friends and k !=self.uid:
            #if k!=self.uid:
                curr_sim = self._getUserSimilarity(my_likes, v.likes_only, k)
                heapq.heappush(minheap, (curr_sim, v))
                if len(minheap) > num:
                    heapq.heappop(minheap)
        return minheap

    def globalRandomUsers(self, allfr, num):
        selected_people = random.sample([(k,v) for k,v in allfr.iteritems()], 1000)
        res = []
        for k, v in selected_people:
            if k not in self.friends and k!=self.uid:
                res.append((1,v))
                if len(res) == num:
                    break
        if len(res) < num:
            print "Error, big error, increase sampling"
            return
        else:
            return res


    def calcGlobalKNearestSim(self,allfr, num, my_likes=None):
        if my_likes is None:
            my_likes = self.likes_only
        minheap = self.globalKNearestSim(allfr, num, my_likes)
        return sum([self._getUserSimilarity(my_likes, uc[1].likes_only, uc[1].uid) for uc in minheap])/float(len(minheap))

    def getUserPopularityAverage(self, artist_dict):
        pop_measure = 0.0
        count = 0
        for aid,created_time in self.likes:
            if aid in artist_dict:
                pop_measure += artist_dict[aid]
                count += 1
        return 0 if count == 0 else pop_measure/count

    def getCirclePopularityAverage(self, artist_dict):
        pop_measure = 0.0
        count = 0
        for k,v in self.fitems.iteritems():
            if k in artist_dict:
                like_count = artist_dict[k]
                pop_measure += like_count * len(v)
                count += len(v)
        return 0 if count==0 else pop_measure/count

    def getCirclePopularityNDCG(self, artist_dict):
        ranked_list = sorted(self.fitems.iteritems(), key=lambda x: len(x[1]), reverse=True)
        dcg = 0.0
        counter = 1
        overall_likes = []
        for itemid, circle_likes in ranked_list:
            if itemid in artist_dict:
                total_numlikes = artist_dict[itemid]
                overall_likes.append((itemid, total_numlikes))
                dcg += (total_numlikes if counter == 1 else total_numlikes/math.log(counter,2))
                counter +=1

        ranked_ideal = sorted(overall_likes, key=operator.itemgetter(1), reverse=True)
        idcg = 0.0
        counter = 1
        for itemid, total_numlikes in ranked_ideal:
             idcg += (total_numlikes if counter ==1 else total_numlikes/math.log(counter, 2))
             counter += 1
        return dcg/idcg

    def getLikesBeforeOnDate(self, bdate):
        return utils.getIdsBeforeOnDate(self.likes, bdate)

    def getLikesAfterDate(self, adate):
        return utils.getIdsAfterDate(self.likes, adate)

    def getCircleNumLikesBefore(self, break_date):
        circle_itemfans = {}
        liked_items = self.getLikesBeforeOnDate(break_date)
        for itemid in liked_items:
            circle_itemfans[itemid] = circle_itemfans.get(itemid, 0) + 1

        #for v1, v2 in self.fitems.iteritems():
        #    circle_itemfans[v1] = circle_itemfans.get(v1,0) + len(utils.getIdsBeforeOnDate(v2, break_date))

        return circle_itemfans

    def getCircleNumLikesAfter(self, break_date):
        circle_itemfans = {}
        liked_items = self.getLikesAfterDate(break_date)
        for itemid in liked_items:
            circle_itemfans[itemid] = circle_itemfans.get(itemid, 0) + 1

        #for v1, v2 in self.fitems.iteritems():
        #    circle_itemfans[v1] = circle_itemfans.get(v1,0) + len(utils.getIdsAfterDate(v2, break_date))

        return circle_itemfans

    def __repr__(self):
        return "User ID: "+str(self.uid) + "\t" + "Likes:" + str(self.getNumLikes()) + "\t" + "Friends:" + str(self.getNumFriends()) + "\n"
        #return "User ID: "+str(self.uid) + "\n" + "Likes:" + str(self.likes) +"\n\n\n" + "".join([("Item Id: "+str(k)+"\n"+"Friend Likers: "+str(v)+"\n") for k,v in self.fitems.iteritems()])a
    """

if __name__ == "__main__":
    a = NetworkNode(1, 1, 1, None)
    print a.sum(1,2)
