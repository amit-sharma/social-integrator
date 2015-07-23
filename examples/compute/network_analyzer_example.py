import getopt
import sys, types
import logging
import time, datetime
import matplotlib as mpl
#mpl.use('Agg')

from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
#from socintpy.networkvisualize.network_visualizer import NetworkVisualizor
from socintpy.networkcompute.locality_analysis2 import LocalityAnalyzer
from socintpy.networkcompute.recommender_analyzer import RecommenderAnalyzer
from socintpy.networkdata.goodreads_preparser import GoodreadsDataPreparser
from socintpy.networkdata.hashtag_data_preparser import HashtagDataPreparser
from socintpy.networkdata.lastfm_data_preparser_csv import LastfmDataPreparserCSV
from socintpy.networkdata.lastfm_data_preparser_simple import LastfmDataPreparserSimple
from socintpy.networkdata.lastfm_data_preparser_lovelisten import LastfmDataPreparserLovelisten
from socintpy.networkdata.flixster_preparser import FlixsterDataPreparser
from socintpy.networkdata.flickr_preparser import FlickrDataPreparser
import socintpy.util.plotter as plotter
import socintpy.util.utils as utils
import socintpy.util.compute_functions as compute
import socintpy.networkdata.generate_fake_data as fake_data
import socintpy.networkcompute.model_adoption as gen_adopt
from scipy.stats import ttest_rel
from pprint import pprint
from time import strftime

COMPATIBLE_DOMAINS = ['twitter', 'lastfm', 'goodreads', 'flixster', 'flickr', 'lastfm_simple', 'lastfm_lovelisten']
AVAILABLE_COMPUTATIONS = ['basic_stats', 'random_similarity', 'knn_similarity', 'knn_recommender', 'circle_coverage',
                          'items_edge_coverage', 'network_draw', 'network_item_adopt', 'node_details', 'store_dataset',
                          'compare_interact_types', 'influence_test', 'suscept_test', 
                          'compute_split_date', 'gen_adopt_data']


def usage():
    print "Too few or erroneous parameters"
    print 'Usage: python '+sys.argv[0]+' -d <dataset_name> -p <path> -c <computation>'

def compare_sims(fr_sim, nonfr_sim):
    diff = ( [(fr_sim[ind][1], nonfr_sim[ind][1], fr_sim[ind][1]-nonfr_sim[ind][1]) for ind in range(len(fr_sim))])
    #pprint(diff)
    print "Circle dominates is", sum([1 for v1,v2,v3 in diff if v3>0])
    print "non-friends dominate is", sum([1 for v1,v2,v3 in diff if v3<0])

def instantiate_networkdata_class(dataset_domain, dataset_path, impl_type, 
                                  max_core_nodes, cutoff_rating, store_dataset, 
                                  interact_type_val, min_interacts_beforeaftersplit_per_user):
    data = None
    #h = hpy()
    #h.setref()
    if dataset_domain == "twitter":
        data = HashtagDataPreparser(dataset_path, impl_type)
    elif dataset_domain== "lastfm":
        data = LastfmDataPreparserCSV(dataset_path, impl_type, cutoff_rating,
                                   max_core_nodes, store_dataset, use_artists=False)
    elif dataset_domain== "lastfm_simple":
        data = LastfmDataPreparserSimple(dataset_path, impl_type, cutoff_rating,
                                   max_core_nodes, store_dataset, use_artists=False, 
                                   interact_type_val=interact_type_val,
                                   min_interactions_per_user=min_interacts_beforeaftersplit_per_user*2)
    elif dataset_domain== "lastfm_lovelisten":
        data = LastfmDataPreparserLovelisten(dataset_path, impl_type, cutoff_rating,
                                   max_core_nodes, store_dataset, use_artists=False, 
                                   interact_type_val=interact_type_val,
                                   min_interactions_per_user=min_interacts_beforeaftersplit_per_user*2)
    elif dataset_domain=="goodreads":
        data = GoodreadsDataPreparser(dataset_path, impl_type, cutoff_rating,
                                      max_core_nodes, store_dataset, 
                                      min_interactions_per_user = min_interacts_beforeaftersplit_per_user*2)
    elif dataset_domain=="flixster":
        data = FlixsterDataPreparser(dataset_path, impl_type, cutoff_rating, 
                                     max_core_nodes, store_dataset, 
                                     min_interactions_per_user=min_interacts_beforeaftersplit_per_user*2)
    elif dataset_domain=="flickr":
        data = FlickrDataPreparser(dataset_path, impl_type, cutoff_rating, 
                                   max_core_nodes, store_dataset,
                                   min_interactions_per_user=min_interacts_beforeaftersplit_per_user*2)
    
    try:
        data.get_all_data()
        BasicNetworkAnalyzer(data).show_basic_stats()
    except:
        raise
    return data


def run_computation(data, computation_cmd, outf, interact_type, create_fake_prefs,
        allow_duplicates, split_date_str, dataset_domain, dataset_path,
        min_interacts_beforeaftersplit_per_user,
        max_interact_ratio_error, max_sim_ratio_error, min_friends_match_ratio, 
        traindata_fraction):
    net_analyzer = BasicNetworkAnalyzer(data)
    interaction_types = data.interact_types_dict
    filename_prefix = computation_cmd if computation_cmd is not None else ""

    if computation_cmd=="basic_stats" or computation_cmd is None:
        net_analyzer.show_basic_stats()
        #data.compute_allpairs_sim(interact_type, data_type=ord("a"))

    elif computation_cmd=="random_similarity":
        for type_name, type_index in interaction_types.iteritems():
            circlesims, globalsims = net_analyzer.compare_circle_global_similarity(type_index, num_random_trials=5, cutoff_rating=cutoff_rating)
            #plotter.plotLinesYY(circlesims, globalsims, "Friends", "Global")
            outf.write("User_id\tcircle_sim\tnonfriend_sim\n")
            outf.write(type_name + '\n')
            for ind in range(len(circlesims)):
                outf.write("%s\t%f\t%f\n" %(circlesims[ind][0], circlesims[ind][1], globalsims[ind][1]))
            print "\n", type_name, ":" 
            print "Circle Average", sum([v2 for v1,v2 in circlesims])/float(len(circlesims))
            print "Global Average", sum([v2 for v1,v2 in globalsims])/float(len(globalsims))

    elif computation_cmd=="knn_similarity":
        #Compute K-nearest similarity
        KLIMITS = [10]
        outf.write("User_id\tk\tcircle_sim\tnonfriend_sim\n")
        
        for type_name, type_index in interaction_types.iteritems():
            for curr_lim in KLIMITS:
                plot_circle, plot_external = net_analyzer.compare_circle_global_knnsimilarity(type_index, klim=curr_lim, cutoff_rating=cutoff_rating)
                compare_sims(plot_circle, plot_external)
                outf.write(type_name+'\n')
                for ind in range(len(plot_circle)):
                    outf.write("%s\t%d\t%f\t%f\n" %(plot_circle[ind][0], curr_lim, plot_circle[ind][1], plot_external[ind][1]))
                #plotter.plotLinesYY(plot_circle, plot_external, "Friends", "Global")
                print type_name, "K", curr_lim
                print "Circle Average", utils.mean_sd([v2 for v1,v2 in plot_circle]), len(plot_circle)
                print "Global Average", utils.mean_sd([v2 for v1,v2 in plot_external]), len(plot_external)

    elif computation_cmd=="knn_recommender":
        #Compute K-nearest recommender
        KLIMITS = [10]
        rec_analyzer = RecommenderAnalyzer(data, max_recs_shown=10, traintest_split=0.7, cutoff_rating=cutoff_rating)
        outf.write("User_id\tk\trun_index\tcircle_ndcg\tnonfriend_ndcg\n")
        for type_name, type_index in interaction_types.iteritems():
            for curr_lim in KLIMITS:
                local_avg=[]
                global_avg=[]
                Ntotal = 10
                for i in range(Ntotal): # randomize because of training-test split.
                    plot_circle, plot_external = rec_analyzer.compare_knearest_recommenders(type_index, klim=curr_lim, num_processes=2)
                    compare_sims(plot_circle, plot_external)
                    outf.write(type_name + "\n")
                    for ind in range(len(plot_circle)):
                        outf.write("%s\t%d\t%d\t%f\t%f\n" %(plot_circle[ind][0], curr_lim, i, plot_circle[ind][1], plot_external[ind][1]))
                    print "\n", type_name, "K", curr_lim

                    #print plot_circle, plot_external
                    curr_avg_local = utils.mean_sd([v2 for v1,v2 in plot_circle])
                    curr_avg_global =  utils.mean_sd([v2 for v1,v2 in plot_external])
                    print "Circle Average", curr_avg_local
                    print "Global Average", curr_avg_global
                    local_avg.append(curr_avg_local[0])
                    global_avg.append(curr_avg_global[0])
                    #plotLinesYY(plot_circle, plot_external, "Friends", "Global")
                print "Local", sum(local_avg)/float(Ntotal)
                print "Global", sum(global_avg)/float(Ntotal)
    elif computation_cmd == "circle_coverage":
        lim_friends = [(5,10), (10,20), (20,50), (50,100)]
        for fr_limit in lim_friends:
            locality_analyzer = LocalityAnalyzer(data)
            coverage_list = locality_analyzer.compare_circle_item_coverages(0, fr_limit[0], fr_limit[1])
            plotter.plotLineY(sorted(coverage_list), "User", "Fraction of Items Covered with %d-%d friends" % (fr_limit[0], fr_limit[1]))
            print utils.mean_sd(coverage_list)
    elif computation_cmd == "items_edge_coverage":
        locality_analyzer = LocalityAnalyzer(data)
        items_cov_list, items_popularity, cov_ratio_list = locality_analyzer.compare_items_edge_coverage(1, minimum_interactions=1)
        print utils.mean_sd(items_cov_list)
        print utils.mean_sd(items_popularity)
        #plotter.plotHist(sorted([val for val in cov_ratio_list if val<=1]), "Ratio of Edge coverage to total popularity", "Frequency", logyscale=True)
        #####plotter.plotHist(sorted([val for val in cov_ratio_list]), "Ratio of Edge coverage to total popularity", "Frequency", logyscale=True)
        #plotter.plotHist(sorted(items_popularity), "Item", "total popularity")
        plotter.plotCumulativePopularity(items_popularity, labelx="Item percentile", labely="Cum. percent of number of likes")
    elif computation_cmd == "network_draw":
        net_visualizor = NetworkVisualizor(data)
        net_visualizor.draw_network()
    elif computation_cmd == "network_item_adopt":
        net_visualizor = NetworkVisualizor(data)
        pprint(net_visualizor.plot_item_adoption(1669118))
    elif computation_cmd == "node_details":
        for node_id in open('user_ids'):
            if node_id.strip('\n') != "User_id":
                net_analyzer.get_node_details(int(node_id.strip('\n')))
    elif computation_cmd=="store_dataset":
        user_interacts = net_analyzer.get_user_interacts(1, cutoff_rating)
        f = open(outf_path+ 'user_interacts_'+dataset_domain+'.tsv', 'w')
        f.write("user_id\titem_id\ttimestamp\n")
        for user_id, item_id, timestamp in user_interacts:
            f.write("%s\t%s\t%s\n" %(user_id, item_id, timestamp)) 
        f.close()
        
        item_pop = net_analyzer.get_items_popularity(1, cutoff_rating)    
        f = open(outf_path+'items_'+dataset_domain+'.tsv','w')
        f.write("item_id\tpopularity\n")
        for item_id, pop in item_pop.iteritems():
            f.write("%s\t%s\n" %(item_id, pop))
        f.close()

        user_friends = net_analyzer.get_user_friends()
        f = open('user_friends_'+dataset_domain+'.tsv','w')
        f.write("user_id\tfriend_id\n")
        for user_id, friend_id in user_friends:
            f.write("%s\t%s\n" %(user_id, friend_id))
        f.close()
        print "Successfully stored tsv dataset"
    elif computation_cmd=="compare_interact_types":
        num_interacts_dict = net_analyzer.compare_interaction_types()
        interact_types = num_interacts_dict.keys()
        plotter.plotLinesYY(num_interacts_dict[interact_types[0]], 
                            num_interacts_dict[interact_types[1]],
                            interact_types[0], interact_types[1], 
                            display=True, logyscale=True)
         
        plotter.plotLinesYY(num_interacts_dict[interact_types[1]], 
                            num_interacts_dict[interact_types[2]],
                            interact_types[1], interact_types[2], 
                            display=True, logyscale=True)
         
        plotter.plotLinesYY(num_interacts_dict[interact_types[0]], 
                            num_interacts_dict[interact_types[2]],
                            interact_types[0], interact_types[2], 
                            display=True, logyscale=True)
    elif computation_cmd=="influence_test":
        #   ta = TemporalAnalyzer(data)
        #interact_type = data.interact_types_dict["listen"
        # time_scale can be 'w':wallclock_time or 'o':ordinal_time
        split_date_str = "2008/01/01"
        t_window = -1
        t_scale = ord('w')
        max_tries_val = 10000
        max_node_computes_val = 100
        max_interact_ratio_error = 0.1
        klim_val=5
        split_timestamp = int(time.mktime(datetime.datetime.strptime(split_date_str, "%Y/%m/%d").timetuple()))
        # crate trainig test sets that will be used by fake geernation
        data.create_training_test_bytime(interact_type, split_timestamp)
        if create_fake_prefs is not None:
            print data.get_nodes_list()[1].get_interactions(interact_type, cutoff_rating=-1)
            fake_data.generate_fake_preferences(data,interact_type, split_timestamp, 
                        min_interactions_beforeaftersplit_per_user=min_interacts_beforeaftersplit_per_user,
                        time_window=t_window, time_scale=t_scale, method=create_fake_prefs)
            
            #fake_data.generate_random_preferences(data, interact_type, split_timestamp)
            print data.get_nodes_list()[1].get_interactions(interact_type, cutoff_rating=-1)
        # Need to generate again because fake data changes test data           
        data.create_training_test_bytime(interact_type, split_timestamp)
        
        la = LocalityAnalyzer(data)
        inf_tuple = compute.test_influence(la, interact_type=interact_type, 
                               time_diff=t_window, time_scale=ord('w'), split_timestamp=split_timestamp, 
                               #time_diff=100000, split_date_str="1970/06/23", 
                               control_divider=0.01,
                               min_interactions_beforeaftersplit_per_user = min_interacts_beforeaftersplit_per_user,
                               max_tries = max_tries_val, max_node_computes=max_node_computes_val, num_processes=4,
                               max_interact_ratio_error=max_interact_ratio_error,
                               klim=klim_val,
                               method="influence")
        print "t-test results", ttest_rel(inf_tuple[2], inf_tuple[3])
        num_vals = len(inf_tuple[0])
        f = open("influence_test", "w")
        for i in range(num_vals):
            f.write("%f\t%f\t%f\t%f\n" % (inf_tuple[0][i], inf_tuple[1][i], 
                        inf_tuple[2][i], inf_tuple[3][i]))
        f.close()
             
    elif computation_cmd=="suscept_test":
        use_artists = "songs" if "songs" in dataset_path else "artists"
        interact_type_str = "listen" if interact_type==0 else "love"
        M = [10]#,20]#,30,40,50]
        t_scale = ord('o') # ordinal scale, this is the default used in paper.
        NUM_NODES_TO_COMPUTE = 4000000 # maximum number nodes to compute?
        num_threads=4 # the number of threads to spawn
        max_tries_val = None#30000 # should we stop after max_tries?
        max_node_computes_val = NUM_NODES_TO_COMPUTE/num_threads # number of nodes to compute at each node
        #max_interact_ratio_error =0.2 # these are errors (defaults are 0.1,0.1)
        #max_sim_ratio_error = 0.2
        #min_friends_match_ratio = 0.5 # important to be 1 for simulation--because e.g. in influence, we use a person's all friends to compute his next like
        klim_val = None # not used for influence test
        nonfr_match = "random" #random, serial, kbest. Default is random.
        num_loop = 1 # number of times we calculate this. For averaging results over multiple runs.
        f = open("suscept_test_results/"+dataset_domain + dataset_path.split("/")[-1] + interact_type_str+ strftime("%Y-%m-%d_%H:%M:%S")+'.dat', 'w')
        f.write("# use_artists=%r\tallow_duplicates=%r\tmax_node_computes_val=%d\tcreate_fake_prefs=%r\tnum_loop=%d\n" % (
                    use_artists, allow_duplicates, max_node_computes_val,
                        create_fake_prefs, num_loop))
        f.write("# split_train_test_date=%s\ttime_scale=%d\tmin_interactions_beforeaftersplit_per_user=%d\tnum_threads=%d\n" % (
                    split_date_str, t_scale, min_interacts_beforeaftersplit_per_user, num_threads))
        f.write("# max_interact_ratio_error=%f\tmax_sim_ratio_error=%f\tmin_friends_match_ratio=%f\n" %(
                    max_interact_ratio_error, max_sim_ratio_error, min_friends_match_ratio
                    ))
        for t_window in M:
            for h in range(num_loop):
                f.write("\n\n################### ALERTINFO: STARTING ITERATION %d  with M=%d\n" %( h, t_window))
                if split_date_str=="test": split_timestamp = 2000
                else:
                    split_timestamp = int(time.mktime(datetime.datetime.strptime(split_date_str, "%Y/%m/%d").timetuple()))
                #split_timestamp=25000000
                if create_fake_prefs is not None:
                    data.create_training_test_bytime(interact_type, split_timestamp)
                    #print data.get_nodes_list()[1].get_interactions(interact_type, cutoff_rating=-1)
                    fake_data.generate_fake_preferences(data,interact_type, split_timestamp,
                            min_interactions_beforeaftersplit_per_user = min_interacts_beforeaftersplit_per_user,
                            time_window=t_window, time_scale=t_scale, method=create_fake_prefs)
                    #print data.get_nodes_list()[1].get_interactions(interact_type, cutoff_rating=-1)
                # Need to generate again because fake data changes test data           
                data.create_training_test_bytime(interact_type, split_timestamp, min_interactions_beforeafter_per_user=min_interacts_beforeaftersplit_per_user)
                la = LocalityAnalyzer(data)
                inf_tuple = compute.test_influence(la, interact_type=interact_type, 
                                       time_diff=t_window, time_scale=t_scale, split_timestamp=split_timestamp, 
                                       #time_diff=100000, split_date_str="1970/06/23", 
                                       control_divider=0.01, # not used anymore
                                       min_interactions_beforeafterspit_per_user = min_interacts_beforeaftersplit_per_user,
                                       max_tries = max_tries_val, max_node_computes=max_node_computes_val, num_threads=num_threads,
                                       max_interact_ratio_error = max_interact_ratio_error,
                                       max_sim_ratio_error = max_sim_ratio_error,
                                       min_friends_match_ratio=min_friends_match_ratio,
                                       klim = klim_val,
                                       nonfr_match=nonfr_match,
                                       method="suscept", 
                                       allow_duplicates=allow_duplicates)
                print "t-test results", ttest_rel(inf_tuple[2], inf_tuple[3])
                num_vals = len(inf_tuple[0])
                f.write("TestSetSize\tFrSimilarity\tNonFrSimilarity\tFrOverlap\tNonFrOverlap\tRandom_run_no\tM\n")
                for i in range(num_vals):
                    f.write("%d\t%f\t%f\t%f\t%f\t%d\t%d\n" % (inf_tuple[0][i], inf_tuple[1][i], 
                                inf_tuple[2][i], inf_tuple[3][i], inf_tuple[4][i], h, t_window))
        f.close()
    elif computation_cmd=="gen_adopt_data":
        t_window = 100 
        t_scale = ord('o')
        if split_date_str=="test": split_timestamp = 2000
        else:
            split_timestamp = int(time.mktime(datetime.datetime.strptime(split_date_str, "%Y/%m/%d").timetuple()))
        if create_fake_prefs is not None:
            data.create_training_test_bytime(interact_type, split_timestamp)
            #print data.get_nodes_list()[1].get_interactions(interact_type, cutoff_rating=-1)
            fake_data.generate_fake_preferences(data,interact_type, split_timestamp,
                    min_interactions_beforeaftersplit_per_user = min_interacts_beforeaftersplit_per_user,
                    time_window=t_window, time_scale=t_scale, method=create_fake_prefs)
        
        data.create_training_test_bytime(interact_type, split_timestamp)
        gen_adopt.generate_adoption_data(data, interact_type, split_timestamp, 
            min_interactions_beforeaftersplit_per_user=min_interacts_beforeaftersplit_per_user, time_window=t_window, 
            time_scale=t_scale)
    elif computation_cmd=="compute_split_date":
        ret_timestamp = compute.compute_cutoff_date(data, interact_type, traindata_fraction)
        print ret_timestamp
        print datetime.datetime.fromtimestamp(ret_timestamp*86400).strftime("%Y-%m-%d")
    """
    elif computation_cmd=="random_recommender":
        for curr_lim in KLIMITS:
            plot_circle, plot_external = compareRandomRecommender(limit=curr_lim)
            print "K", curr_lim
            print "Circle Average", sum(plot_circle)/float(len(plot_circle))
            print "Global Average", sum(plot_external)/float(len(plot_external))
    elif computation_cmd=="sparsity":
        circle_sparsity, global_sparsity = computeLocalGlobalSparsity(data)
        print "Local Sparsity", circle_sparsity
        print "Global sparsity", global_sparsity
    elif computation_cmd == "item_popularity":
        #mean, variance, median = getItemPopularityAverage(data)
        distr_list, items_covered_ratio, item_likecounts = getItemPopularityInDataset(data)
        #plotScatterXY([k for k,v in distr_list],[math.log10(v) for k,v in distr_list], "Number of times a hashtag is used", "Log(Frequency)", xlimit=[0,225])
        #plotLineY(sorted([math.log10(val) for val in item_likecounts], reverse=True))
        #print "Average item popularity", mean
        #print "Variance of item popularity", variance
        #print "Median item popularity", median

        print "Percentage of items convered in plot", items_covered_ratio
        items_likes_str=str(item_likecounts[0])
        for val in item_likecounts[1:]:
            items_likes_str+=' '+str(val)
        items_likes_str +="\n"
        open('graph_itempopularity','a').write(items_likes_str)
        distr_list_str = ""
        for k, v in distr_list:
            distr_list_str += (str(k)+":"+str(v)+" ")
        distr_list_str += "\n"
        open('distr_item.dat','a').write(distr_list_str)
    elif computation_cmd == "user_likes":
        distr_list, items_covered_ratio = getUserLikesInDataset(data)
        plotScatterXY([k for k,v in distr_list],[math.log10(v) for k,v in distr_list], "Number of hashtags used by a user", "Log(Frequency)")
        print "Percentage of users convered in plot", items_covered_ratio
    elif computation_cmd == "item_circle_coverage":
        mean, freq_circles, dummy = getCircleItemCoverage(data)
        print "Average circle coverage for an item", mean
        print "Frequency-wise circle coverage for an item", freq_circles
    elif computation_cmd == "random_network_coverage":
        full_avg = 0.0
        for i in range(10):
            ratio_1, ratio_2 = compareRandomNetworkCoverage(data, random_type="randomize_edges")
            full_avg += ratio_1
            print "MyMetric:", ratio_1
            print "OtherMetric", ratio_2
        print "\nFull Average", full_avg/10
    elif computation_cmd=="numfr_commoncircle":
        plotNumFriendsVsCommonCircleItems(users)
    elif computation_cmd=="compare_user_popularity":
        #Compute average popularity in circles, compare with global
        pop_circle = getCirclePopularityAverages(data)
        pop_user = getUserPopularityAverages(data)
        #plotLineY(pop_circle)
        plotLinesYY(pop_circle, pop_user, "Circle", "Root User", "User Index", "Avg. Popularity")
    elif computation_cmd=="ndcg_popularity":
        #Compute NDCG of popularity of circle wrt global (I think)
        uc_ndcg = getCirclePopularityNDCGs(data)
        plotLineY(uc_ndcg, "User Index", "Popularity NDCG")
    elif computation_cmd=="tiestrength_similarity":
        # See the effect of tie strength
        tiestrength_pairs = getUserFrTieStrength()
        similarity_arr = getUserFrSimilarity(users, tiestrength_pairs)
        plotScatterXY([tie for u1,u2,tie in tiestrength_pairs], similarity_arr, "TieRank", "Similarity")
    elif computation_cmd=="diffusion":
        # See diffusion
        kval, probs = computeDiffusionProbabilities(users, date(2010,12,31))
        plotScatterXY(kval, probs, "K", "Probability")
    elif computation_cmd=="fans_before_after":
        fans_before, fans_after = computeItemGrowth(data.users, date(2010,12,31))
        plotLineY([fans_after[i]/float(fans_before[i]) for i in range(len(fans_before))], "Items", "Popularity")

    """ 
if __name__ == "__main__":
    logging.basicConfig(filename="run.log", level="DEBUG")
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d:c:p:", ["help", "cython", 
                                   "cutoffrating=", "output=", "max_core_nodes=",
                                   "store_dataset", "interact_type=", 
                                   "min_interactions_beforeaftersplit_per_user=", 
                                   "create_fake_prefs=", "split_date=",
                                   "no_duplicates", "max_numinteracts_ratio_error=",
                                   "max_sim_ratio_error=", 
                                   "min_numfriends_match_ratio=", "traindata_fraction="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    if not opts:
        usage()
        sys.exit(2)


    dataset_domain=None
    dataset_path = None
    computation_cmd = None
    max_core_nodes = None
    impl_type = "python"
    # previously, None # i.e. cutoff has no effect. This value may be overridden 
    # by future methods..but the filtering on cutoff has already taken place 
    # when creating the data structure in memory
    cutoff_rating = -1 
    store_dataset = False
    interact_type = 0
    min_interactions_beforeaftersplit_per_user = 1
    create_fake_prefs = None
    no_duplicates = False
    split_date_str = None
    traindata_fraction = None
    min_friends_match_error = None
    max_interact_ratio_error = None
    max_sim_ratio_error = None
    for o, a in opts: 
        if o =="-d":
            if a not in COMPATIBLE_DOMAINS:
                print a
                assert False, "Unhandled dataset domain."
            else:
                dataset_domain = a
        if o =="-p":
            dataset_path = a
        if o == "-c":
            if a not in AVAILABLE_COMPUTATIONS:
                assert False, "Unhandled computation."
            else:
                computation_cmd = a
        if o == "--cython":
            impl_type = "cython"
        if o =="--cutoffrating":
            cutoff_rating = int(a)
        if o=="--max_core_nodes":
            max_core_nodes = int(a)
        if o=="--store_dataset":
            store_dataset = True
        if o=="--interact_type":
            interact_type = int(a)
        if o=="--min_interactions_beforeaftersplit_per_user":
            min_interactions_beforeaftersplit_per_user = int(a)
        if o=="--create_fake_prefs":
            create_fake_prefs = a
        if o=="--no_duplicates":
            no_duplicates = True
        if o=="--split_date":
            split_date_str= a
        if o=="--max_numinteracts_ratio_error":
            max_interact_ratio_error = float(a)
        if o == "--max_sim_ratio_error":
            max_sim_ratio_error = float(a)
        if o == "--min_numfriends_match_ratio":
            min_friends_match_error = float(a)
        if o == "--traindata_fraction":
            traindata_fraction = float(a)

    allow_duplicates = not no_duplicates
    if dataset_domain is None or dataset_path is None:
        usage()
        sys.exit(2)

    # supports interactive runs. Checks if data already fetched in an earlier 
    # run of the script. 
    if 'data' not in globals():
        data = instantiate_networkdata_class(dataset_domain, dataset_path, impl_type, 
                                       max_core_nodes, cutoff_rating, store_dataset, 
                                        interact_type, min_interactions_beforeaftersplit_per_user)
    outf=open("current_run.dat", 'w')
    run_computation(data, computation_cmd, outf, interact_type, create_fake_prefs,
            allow_duplicates, split_date_str, dataset_domain, dataset_path,
            max_interact_ratio_error, max_sim_ratio_error, min_friends_match_error,
            traindata_fraction)
    outf.close()

def get_data(from_raw_dataset=False):
    if from_raw_dataset:
        dataset_domain="lastfm"
        dataset_path="/mnt/bigdata/lastfm/"
    else:
        dataset_domain="lastfm_simple"
        dataset_path = "/mnt/bigdata/lastfm_ego/ego_songs_75_110kdata/"
    computation_cmd = "basic_stats"
    max_core_nodes = None
    impl_type = "cython"
    cutoff_rating = None # sufficiently small value so that cutoff has no effect.
    store_dataset = True

    data = instantiate_networkdata_class(dataset_domain, dataset_path, impl_type, 
                                  max_core_nodes, cutoff_rating, store_dataset)

    return data

