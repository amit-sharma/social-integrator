import getopt
import sys
import logging

import matplotlib as mpl
mpl.use('Agg')
from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
from socintpy.networkvisualize.network_visualizer import NetworkVisualizor
from socintpy.networkcompute.locality_analysis import LocalityAnalyzer
from socintpy.networkcompute.recommender_analyzer import RecommenderAnalyzer
from socintpy.networkdata.goodreads_preparser import GoodreadsDataPreparser
from socintpy.networkdata.hashtag_data_preparser import HashtagDataPreparser
from socintpy.networkdata.lastfm_data_preparser import LastfmDataPreparser
from socintpy.networkdata.flixster_preparser import FlixsterDataPreparser
from socintpy.networkdata.flickr_preparser import FlickrDataPreparser
import socintpy.util.plotter as plotter
import socintpy.util.utils as utils
from pprint import pprint

COMPATIBLE_DOMAINS = ['twitter', 'lastfm', 'goodreads', 'flixster', 'flickr']
AVAILABLE_COMPUTATIONS = ['basic_stats', 'random_similarity', 'knn_similarity', 'knn_recommender', 'circle_coverage',
                          'items_edge_coverage', 'network_draw', 'network_item_adopt', 'node_details', 'store_dataset']
def usage():
    print "Too few or erroneous parameters"
    print 'Usage: python '+sys.argv[0]+' -d <dataset_name> -p <path> -c <computation>'

def compare_sims(fr_sim, nonfr_sim):
    diff = ( [(fr_sim[ind][1], nonfr_sim[ind][1], fr_sim[ind][1]-nonfr_sim[ind][1]) for ind in range(len(fr_sim))])
    #pprint(diff)
    print "Circle dominates is", sum([1 for v1,v2,v3 in diff if v3>0])
    print "non-friends dominate is", sum([1 for v1,v2,v3 in diff if v3<0])

if __name__ == "__main__":
    logging.basicConfig(filename="run.log", level="DEBUG")
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d:c:p:", ["help", "cython", "cutoffrating=", "output="])
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
    impl_type = "python"
    cutoff_rating = None # sufficiently small value so that cutoff has no effect.
    for o, a in opts:
        if o =="-d":
            if a not in COMPATIBLE_DOMAINS:
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

    if dataset_domain is None or dataset_path is None:
        usage()
        sys.exit(2)

    data = None
    #h = hpy()
    #h.setref()
    if dataset_domain == "twitter":
        data = HashtagDataPreparser(dataset_path, impl_type)
    elif dataset_domain== "lastfm":
        data = LastfmDataPreparser(dataset_path+"lastfm_nodes.db", dataset_path+"lastfm_edges.db", impl_type)
    elif dataset_domain=="goodreads":
        data = GoodreadsDataPreparser(dataset_path, impl_type, cutoff_rating)
    elif dataset_domain=="flixster":
        data = FlixsterDataPreparser(dataset_path, impl_type, cutoff_rating)
    elif dataset_domain=="flickr":
        data = FlickrDataPreparser(dataset_path, impl_type, cutoff_rating=None)
    
    try:
        data.get_all_data()
    except:
        raise
    #print h.heap()
    #sys.exit(0)
    net_analyzer = BasicNetworkAnalyzer(data)
    interaction_types = data.interact_types_dict
    filename_prefix = computation_cmd if computation_cmd is not None else ""
    #outf_path = "/home/asharma/datasets/processed/" + dataset_domain + "/" + filename_prefix
    #outf = open(outf_path + "_output.tsv", "w")
    if computation_cmd=="basic_stats" or computation_cmd is None:
        net_analyzer.show_basic_stats()

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
