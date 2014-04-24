from collections import namedtuple
import heapq
import operator
import math, random
import utils


class NetworkNode():
    user_sim = {}

    def __init__(self, uid, node_data):
        self.uid = uid
        self.node_data = node_data
        self.interactions = {}

    def register_interactions(self, interaction_name):
      self.interactions[interaction_name] = {}

    def add_interaction(self, interaction_name, interaction_id, interaction_data):
      self.interactions[interaction_name][interaction_id] = {'id': interaction_id, 'interaction_data':interaction_data}

    def add_friend(self, friendid, friend_data):
      self.friends[friendid] = {'id':friendid, 'friend_data':friend_data}

    def createFriendSet(self):
        self.friends = set()
        for k,v in self.fitems.iteritems():
            for friendid, created_time in v:
                self.friends.add(friendid)
        return self.friends
    """
    def createLikesOnlySet(self):
        self.likes_only = set()
        for k, created in self.likes:
            self.likes_only.add(k)
        return self.likes_only

    def getTotalItems(self):
        length_fitems = sum([len(v) for k,v in self.fitems.iteritems()])
        return len(self.likes) + length_fitems

    def circleSimilarity(self):
        return self.externalSimilarity(self.fitems)

    def circleNonWeightedSimilarity(self):
        return self.externalNonWeightedSimilarity(self.fitems)

    def externalSimilarity(self, items):
        simscore=float(0)
        for k, created_time in self.likes:
            if k in items:
                simscore += len(items[k])
        return simscore/(len(self.likes)*self.getNumFriends())

    def externalNonWeightedSimilarity(self, items):
        simscore=float(0)
        for k,created_time in self.likes:
            if k in items:
                simscore += 1
        return simscore/(len(self.likes))

    def getNumFriends(self):
        return len(self.friends)

    def getNumLikes(self):
        return len(self.likes)

    def getAllCircleItems(self):
        items = []
        items.extend([v1 for v1, v2 in self.likes])
        items.extend(self.fitems.keys())
        return set(items)

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

    def _getUserSimilarity(self, my_likes, other_likes, other_uid):
        #if (self.uid, other_uid) in UCircle.user_sim:
        #    return UCircle.user_sim[(self.uid, other_uid)]

        simscore = len(my_likes & other_likes)
        unionsum = len(my_likes) + len(other_likes) - simscore
        usersim = simscore/float(unionsum)

        #if (self.uid, other_uid) in UCircle.user_sim:
        #    print UCircle.user_sim[(self.uid, other_uid)], usersim, self.uid, other_uid
        #    pprint(UCircle.user_sim)
        #UCircle.user_sim[(self.uid, other_uid)] = usersim
        return usersim

    def circleKNearestSim(self, allfr, num, my_likes):
        minheap=[]
        for frid in self.friends:
            curr_sim = self._getUserSimilarity(my_likes, allfr[frid].likes_only, frid)
            heapq.heappush(minheap, (curr_sim, allfr[frid]))
            if len(minheap) > num:
                heapq.heappop(minheap)
        return minheap

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