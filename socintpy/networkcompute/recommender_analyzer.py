import socintpy.networkcompute.network_recommender as recsys
from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer


class RecommenderAnalyzer(BasicNetworkAnalyzer):
    def __init__(self, networkdata, max_recs_shown, traintest_split):
        super(RecommenderAnalyzer, self).__init__(networkdata)
        self.traintest_split = traintest_split
        self.max_recs_shown = max_recs_shown

    def compare_knearest_recommenders(self, interact_type, klim=None):
        comm1 = []
        comm2 = []
        for v in self.netdata.get_core_nodes_iterable(should_have_friends=True, should_have_interactions=True):
            if len(v.interactions[interact_type])>1: # minimum required to split data in train and test set
                ev = recsys.RecAlgoEvaluator(v, interact_type = interact_type, split=self.traintest_split)
                traindata, testdata = ev.create_training_test_sets()

                r1 = recsys.CircleKNearestRecommender(v, self.netdata, ev, interact_type=interact_type, K=klim,
                                                      max_items=self.max_recs_shown)
                r1.recommend()

                r2 = recsys.GlobalKNearestRecommender(v, self.netdata, ev, interact_type=interact_type, K=klim,
                                                      max_items=self.max_recs_shown)
                r2.recommend()
                #print "Circle recommendations:", v.uid,r1.recommend()
                #print "Global recommendations", v.uid, r2.recommend()
                comm1.append(r1.getNDCG())
                comm2.append(r2.getNDCG())
                #print "User l:wikes: ", v.likes, len(v.friends), comm1[len(comm1)-1], comm2[len(comm2)-1], "\n"
        return comm1, comm2
