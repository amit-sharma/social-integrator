import socintpy.networkcompute.network_recommender as recsys
from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
import logging
from multiprocessing import Pool, Process, Queue

#g_netdata= None
#g_interact_type = None
#g_klim = None
#g_max_recs_shown = None
#g_nodes_list = None

def process_nodes(partial_nodes_list, g_netdata, g_interact_type, g_klim, g_max_recs_shown, q):
#def process_nodes(partial_nodes_list):
#def process_nodes(v_index):
    print "Started process"
    #v = g_nodes_list[v_index]
    #ret_local = None
    #ret_global = None
    local_ret_list = []
    global_ret_list = []
    counter = 0
    for v in partial_nodes_list:
        if counter % 1000 == 0:
            print "Starting", counter
        counter += 1
        # TEST code here: to see the effect of number of interactions
        if v.length_train_ids >0 and v.length_test_ids > 0:# and v.get_num_interactions(g_interact_type)<30:
            r1 = recsys.CircleKNearestRecommender(v, g_netdata, interact_type=g_interact_type, K=g_klim,
                                                  max_items=g_max_recs_shown)
            ret1 = r1.recommend()
            if not ret1: # empty or is None
                logging.warning("Error in computing local recommendations for %d" % v.uid)
                #return (-1, -1)
            else:
                ret_local = r1.getNDCG()
                r2 = recsys.GlobalKNearestRecommender(v, g_netdata, interact_type=g_interact_type, K=g_klim,
                                                  max_items=g_max_recs_shown)
                ret2 = r2.recommend() # empty or is None
                if not ret2:
                    logging.warning("Error in computing global recommendations for %d" % v.uid)
                    #return (-1, -1)
                else:
                    ret_global = r2.getNDCG()
                    local_ret_list.append((v.uid, ret_local))
                    global_ret_list.append((v.uid, ret_global))
                    
                    #      print "Circle recommendations:", v.uid,r1.recommend()
                    #print "Global recommendations", v.uid, r2.recommend()
                    #ret_local.append(r1.getNDCG())
                    #ret_global.append(r2.getNDCG())
                    #print "User l:wikes: ", v.likes, len(v.friends), comm1[len(comm1)-1], comm2[len(comm2)-1], "\n"
    print "Ended process"
    #return (ret_local, ret_global)
    q.put((local_ret_list, global_ret_list))
    return local_ret_list, global_ret_list

 
class RecommenderAnalyzer(BasicNetworkAnalyzer):
    def __init__(self, networkdata, max_recs_shown, traintest_split, cutoff_rating):
        #global g_max_recs_shown, g_netdata
        super(RecommenderAnalyzer, self).__init__(networkdata)
        self.traintest_split = traintest_split
        self.max_recs_shown = max_recs_shown
        self.cutoff_rating = cutoff_rating
        #g_max_recs_shown = max_recs_shown
        #g_netdata = networkdata

    def compare_knearest_recommenders(self, interact_type, klim, num_processes):
        #global g_interact_type, g_klim, g_nodes_list
        comm1 = []
        comm2 = []
        i = 0
        #g_interact_type = interact_type
        #g_klim = klim
        # all nodes that have interactions (core or non-core) should have train set
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True):
            if v.get_num_interactions(interact_type) > 1:  # minimum required to split data in train and test set
                #ev = recsys.RecAlgoEvaluator(v, interact_type = interact_type, split=self.traintest_split)
                #traindata, testdata = ev.create_training_test_sets()
                v.create_training_test_sets(interact_type, self.traintest_split, self.cutoff_rating)

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
        partial_num_nodes = len(nodes_list) / num_processes + 1
        arg_list = []
        for i in range(num_processes):
            start_index = i * partial_num_nodes
            end_index = min((i + 1) * partial_num_nodes, len(nodes_list))
            arg_list.append(nodes_list[start_index:end_index])
        
        #pool = Pool(processes=num_processes)
        #print len(nodes_list)
        #param_iterable = map(arg_list, [self.netdata]*num_processes, [interact_type]*num_processes,
        #               [klim]*num_processes, [self.max_recs_shown]*num_processes)
        #res_list = pool.map(process_nodes, range(len(nodes_list)), chunksize=10000)
        #pool.close()
        #pool.join()
        #q = queue
        #"""
        q = Queue()
        proc_list = []
        for i in range(num_processes):
            p = Process(target=process_nodes, args=(arg_list[i],self.netdata, interact_type,klim, self.max_recs_shown, q))
            p.start()
            proc_list.append(p)
        for p in proc_list:
            local_curr_list, global_curr_list = q.get()
            comm1.extend(local_curr_list)
            comm2.extend(global_curr_list)
        for p in proc_list:
            p.join()
        """
        for i in range(num_processes):
            process_nodes(arg_list[i], self.netdata, interact_type, klim, self.max_recs_shown)
        """
        """
        for ret_local, ret_global in res_list:
            if ret_local is not None and ret_global is not None:
                comm1.append(ret_local)
                comm2.append(ret_global)
        """
        #pool = Pool(processes=4)
        #pool.map()
        return comm1, comm2
