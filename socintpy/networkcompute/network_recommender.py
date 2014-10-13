from random import random
import heapq
from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
import socintpy.util.utils as utils
import operator
import math
import logging

class RecAlgoEvaluator:
    def __init__(self, uc, interact_type, split):
        self.usercircle = uc
        self.train_likes = {}
        self.test_likes = {}
        self.traintestsplit = split
        self.interact_type = interact_type

    """
    #TODO adhoc step here to transfer a value to the test set. think of a better way.
    def create_training_test_sets(self):
        for item_id in self.usercircle.get_items_interacted_with(self.interact_type):
            if random() < self.traintestsplit:
                self.train_likes[item_id] = True
            else:
                self.test_likes[item_id] = True
        
        #prevent empty test set
        # TODO: not really random right now!
        if len(self.test_likes) == 0:
            random_key = self.train_likes.keys()[0]
            elem = self.train_likes.pop(random_key)
            self.test_likes[random_key] = elem
            
        return self.train_likes, self.test_likes
    """

class Recommender:
    def __init__(self, uc, netdata, interact_type, maxitems):
        self.usercircle=uc
        self.netdata = netdata
        #self.evaluator = evaluator
        self.interact_type = interact_type
        if not maxitems:
            self.max_items = 10
        else:
            self.max_items = maxitems
        self.rec_items = None
        
    def recommend(self):
        raise NotImplementedError
    
    def _selectWeightedPopularRecsFromUsers(self, close_users):
        item_count = {}
        for sim_score, uc in close_users:
            for itemid, interaction_data in uc.interactions[self.interact_type].iteritems():
                if itemid not in self.evaluator.train_likes:   #recs cant be same as past interactions
                    if itemid not in item_count:
                        item_count[itemid] = 0
                    item_count[itemid] += sim_score
        if self.max_items >= len(item_count):
            topk_items = list(item_count.iteritems())
        else:
            topk_items = utils.selectTopK(list(item_count.iteritems()), self.max_items)
        rec_tuples = sorted(topk_items, key=operator.itemgetter(1), reverse=True)
        return [v1 for v1, v2 in rec_tuples]

    #TODO: some arbitrary decisions here while computing precision denominator, min functions in NDCG. Check them.
    def getPrecision(self):
        #print self.test_likes, self.rec_items
        common_items = len(set(self.evaluator.test_likes.keys()) & set(self.rec_items))
        return common_items if common_items ==0 else common_items/float(len(self.rec_items))
    
    def getRecall(self):
        common_items = len(set(self.evaluator.test_likes.keys()) & set(self.rec_items))
        return common_items if common_items ==0 else common_items/float(len(self.evaluator.test_likes))
    
    def getFMeasure(self):
        p = self.getPrecision()
        r = self.getRecall()
        return 0 if (p+r) == 0 else (2*p*r)/(p+r)
    
    def getNDCG(self):
        dcg =0
        i = 1
        for itemid in self.rec_items:
            if self.usercircle.in_test_set(itemid):
                rel = 1
            else:
                rel = 0
            dcg += (rel if i==1 else (rel/math.log(i,2)))
            i += 1
        # A little redundant because rec_items are constrained to be <=max_items
        idcg_size = min(self.max_items, len(self.rec_items), self.usercircle.length_test_ids) # ideal DCG
        idcg = 0
        for i in range(1,idcg_size+1):
            idcg += (1 if i==1 else (1/math.log(i,2)))
        if idcg > 0:
            #print self.max_items, len(self.rec_items), self.usercircle.length_test_ids, dcg/float(idcg)
            return dcg/float(idcg)
        else:
            logging.error("Critical Error, how can idcg be zero!!!\n\n\n\n")
            print("Critical Error, how can idcg be zero!!!\n")
            print self.max_items, len(self.rec_items), self.usercircle.length_test_ids, dcg, float(idcg)
            return 0.0
        
class CircleKNearestRecommender(Recommender):
    def __init__(self, uc, netdata, interact_type, K=50, max_items=None):
        Recommender.__init__(self, uc, netdata, interact_type, max_items)
        self.K = K
    
    def recommend(self):
        close_users = BasicNetworkAnalyzer.compute_knearest_neighbors(self.usercircle,
                                                                      self.netdata.get_friends_nodes(self.usercircle),
                                                                      self.interact_type, self.K,data_type="learn")
        #print "Num close users", len(close_users), "Num friends", self.usercircle.get_num_friends()
        if len(close_users)< self.K:
            logging.warning("Cannot find k closest friends for recommend")
            return None                                                                  
        self.rec_items = self.usercircle.compute_weighted_popular_recs(close_users,self.max_items) 
        
        """
        if len(self.rec_items) == 0:
            print "oh"
            for sim, unode in close_users:
                print unode.length_train_ids
        """
        return self.rec_items

class CircleRandomRecommender(Recommender):
    def __init__(self, uc, allfr, evaluator, K=50, max_items=None):
        Recommender.__init__(self, uc, allfr, evaluator, max_items)
        self.K = K
    
    def recommend(self):
        close_users = self.usercircle.circleRandomUsers(self.allfr, self.K)
        self.rec_items = self._selectWeightedPopularRecsFromUsers(close_users)
        return self.rec_items

class GlobalKNearestRecommender(Recommender):
    def __init__(self, uc, netdata, interact_type, K=50, max_items=None):
        Recommender.__init__(self, uc, netdata, interact_type, max_items)
        self.K = K
        
    def recommend(self):
        close_users = BasicNetworkAnalyzer.compute_knearest_neighbors(self.usercircle,
                                                                      self.netdata.get_nonfriends_nodes(self.usercircle),
                                                                      self.interact_type, self.K, data_type="learn"
                                                                      )
        
        #print "Num close users", len(close_users)
        if len(close_users) < self.K:
            logging.warning("Cannot find k closest global for recommend")
            return None
        self.rec_items = self.usercircle.compute_weighted_popular_recs(close_users, self.max_items)
       
        """
        Length of recs can be zero because there are so little train_interactions of the close users 
        if len(self.rec_items) == 0:
            print "oh"
            for sim, unode in close_users:
                print unode.length_train_ids
        """
        return self.rec_items

class GlobalRandomRecommender(Recommender):
    def __init__(self, uc, allfr, evaluator, K=50, max_items=None):
        Recommender.__init__(self, uc, allfr, evaluator, max_items)
        self.K = K
        
    def recommend(self):
        close_users = self.usercircle.globalRandomUsers(self.allfr, self.K)
        self.rec_items = self._selectWeightedPopularRecsFromUsers(close_users)
        return self.rec_items

