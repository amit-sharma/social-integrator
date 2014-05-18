import socintpy.networkcompute.network_recommender as recsys
from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
import logging
from multiprocessing import Pool


def process_nodes(partial_nodes_list, netdata, interact_type, klim, max_recs_shown):
    print "Started process"
    ret_local = []
    ret_global = []
    for v in partial_nodes_list:
        if v.get_num_interactions(interact_type) > 1:
            r1 = recsys.CircleKNearestRecommender(v, netdata, interact_type=interact_type, K=klim,
                                                  max_items=max_recs_shown)
            ret1 = r1.recommend()
            if ret1 is None:
                logging.warning("Error in computing local recommendations for %d" % v.uid)
                continue

            r2 = recsys.GlobalKNearestRecommender(v, netdata, interact_type=interact_type, K=klim,
                                                  max_items=max_recs_shown)
            ret2 = r2.recommend()
            if ret2 is None:
                logging.warning("Error in computing global recommendaitons for %d" % v.uid)
                continue
            #print "Circle recommendations:", v.uid,r1.recommend()
            #print "Global recommendations", v.uid, r2.recommend()
            ret_local.append(r1.getNDCG())
            ret_global.append(r2.getNDCG())
            #print "User l:wikes: ", v.likes, len(v.friends), comm1[len(comm1)-1], comm2[len(comm2)-1], "\n"
    print "Ended process"
    return (ret_local, ret_global)


class RecommenderAnalyzer(BasicNetworkAnalyzer):
    def __init__(self, networkdata, max_recs_shown, traintest_split):
        super(RecommenderAnalyzer, self).__init__(networkdata)
        self.traintest_split = traintest_split
        self.max_recs_shown = max_recs_shown

    def compare_knearest_recommenders(self, interact_type, klim=None):
        comm1 = []
        comm2 = []
        i = 0
        for v in self.netdata.get_nodes_iterable(should_have_friends=True, should_have_interactions=True):
            if v.get_num_interactions(interact_type) > 1:  # minimum required to split data in train and test set
                #ev = recsys.RecAlgoEvaluator(v, interact_type = interact_type, split=self.traintest_split)
                #traindata, testdata = ev.create_training_test_sets()
                v.create_training_test_sets(interact_type, self.traintest_split)

        """
        for v in self.netdata.get_nodes_iterable(should_have_friends=True, should_have_interactions=True):
            if i % 1000 == 0:
                print "Starting ", i
            i += 1
            if v.get_num_interactions(interact_type)>1:
                r1 = recsys.CircleKNearestRecommender(v, self.netdata, interact_type=interact_type, K=klim,
                                                      max_items=self.max_recs_shown)
                ret1 = r1.recommend()
                if ret1 is None:
                    logging.warning("Error in computing local recommendations for %d" %v.uid)
                    continue

                r2 = recsys.GlobalKNearestRecommender(v, self.netdata, interact_type=interact_type, K=klim,
                                                      max_items=self.max_recs_shown)
                ret2 = r2.recommend()
                if ret2 is None:
                    logging.warning("Error in computing global recommendaitons for %d" %v.uid)
                    continue
                #print "Circle recommendations:", v.uid,r1.recommend()
                #print "Global recommendations", v.uid, r2.recommend()
                comm1.append(r1.getNDCG())
                comm2.append(r2.getNDCG())
                #print "User l:wikes: ", v.likes, len(v.friends), comm1[len(comm1)-1], comm2[len(comm2)-1], "\n"
        """
        nodes_list = self.netdata.get_nodes_list(should_have_friends=True, should_have_interactions=True)
        num_processes = 4
        partial_num_nodes = len(nodes_list) / 4 + 1
        arg_list = []
        for i in range(num_processes):
            start_index = i * partial_num_nodes
            end_index = min((i + 1) * partial_num_nodes, len(nodes_list))
            arg_list.append(nodes_list[start_index:end_index])

        res_list = map(process_nodes, arg_list, [self.netdata]*num_processes, [interact_type]*num_processes,
                       [klim]*num_processes, [self.max_recs_shown]*num_processes)
        for ret_process in res_list:
            comm1.extend(ret_process[0])
            comm2.extend(ret_process[1])
        #pool = Pool(processes=4)
        #pool.map()
        return comm1, comm2
