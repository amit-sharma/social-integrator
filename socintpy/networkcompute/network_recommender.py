from random import random
import heapq
from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
import socintpy.util.utils as utils
import operator
import math

class RecAlgoEvaluator:
    def __init__(self, uc, interact_type, split):
        self.usercircle = uc
        self.train_likes = {}
        self.test_likes = {}
        self.traintestsplit = split
        self.interact_type = interact_type
    #TODO adhoc step here to transfer a value to the test set. think of a better way.
    def create_training_test_sets(self):
        for k, interaction_data in self.usercircle.interactions[self.interact_type].iteritems():
            if random() < self.traintestsplit:
                self.train_likes[k] = interaction_data
            else:
                self.test_likes[k] = interaction_data
        
        #prevent empty test set
        # TODO: not really random right now!
        if len(self.test_likes) == 0:
            random_key = self.train_likes.keys()[0]
            elem = self.train_likes.pop(random_key)
            self.test_likes[random_key] = elem
            
        return self.train_likes, self.test_likes
        
class Recommender:
    def __init__(self, uc, netdata, evaluator, interact_type, maxitems):
        self.usercircle=uc
        self.netdata = netdata
        self.evaluator = evaluator
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
            if itemid in self.evaluator.test_likes:
                rel = 1
            else:
                rel = 0
            dcg += (rel if i==1 else (rel/math.log(i,2)))
            i += 1
        idcg_size = min(self.max_items, len(self.rec_items), len(self.evaluator.test_likes)) # ideal DCG
        idcg = 0
        #print self.max_items, self.test_likes, self.usercircle.likes
        for i in range(1,idcg_size+1):
            idcg += (1 if i==1 else (1/math.log(i,2)))
        if idcg > 0:
            return dcg/float(idcg)
        else:
            print "Critical Error, how can idcg be zero!!!\n\n\n\n"
            return 0.0
        
class CircleKNearestRecommender(Recommender):
    def __init__(self, uc, netdata, evaluator, interact_type, K=50, max_items=None):
        Recommender.__init__(self, uc, netdata, evaluator, interact_type, max_items)
        self.K = K
    
    def recommend(self):
        close_users = BasicNetworkAnalyzer.compute_knearest_neighbors(self.usercircle, self.usercircle.get_friendnodes_iterable(),
                                                                      self.interact_type, self.K,
                                                                      self.evaluator.train_likes)
        self.rec_items = self._selectWeightedPopularRecsFromUsers(close_users)
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
    def __init__(self, uc, netdata, evaluator, interact_type, K=50, max_items=None):
        Recommender.__init__(self, uc, netdata, evaluator, interact_type, max_items)
        self.K = K
        
    def recommend(self):
        close_users = BasicNetworkAnalyzer.compute_knearest_neighbors(self.usercircle,
                                                                      self.netdata.get_nonfriends_iterable(self.usercircle),
                                                                      self.interact_type, self.K,
                                                                      self.evaluator.train_likes)
        self.rec_items = self._selectWeightedPopularRecsFromUsers(close_users)
        return self.rec_items

class GlobalRandomRecommender(Recommender):
    def __init__(self, uc, allfr, evaluator, K=50, max_items=None):
        Recommender.__init__(self, uc, allfr, evaluator, max_items)
        self.K = K
        
    def recommend(self):
        close_users = self.usercircle.globalRandomUsers(self.allfr, self.K)
        self.rec_items = self._selectWeightedPopularRecsFromUsers(close_users)
        return self.rec_items

