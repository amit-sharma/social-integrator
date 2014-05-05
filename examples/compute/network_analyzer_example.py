from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
from socintpy.networkcompute.recommender_analyzer import RecommenderAnalyzer
from socintpy.networkdata.hashtag_data_preparser import HashtagDataPreparser
from socintpy.networkdata.lastfm_data_preparser import LastfmDataPreparser
import  socintpy.util.plotter as plotter
import socintpy.util.utils as utils
import getopt,sys

COMPATIBLE_DOMAINS = ['twitter', 'lastfm', 'goodreads']
AVAILABLE_COMPUTATIONS = ['basic_stats', 'random_similarity', 'knn_similarity', 'knn_recommender']
def usage():
    print "Too few or erroneous parameters"
    print 'Usage: python '+sys.argv[0]+' -d <dataset> -p <path>'

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d:c:p:", ["help", "output="])
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

    if dataset_domain is None or dataset_path is None:
        usage()
        sys.exit(2)

    data = None
    if dataset_domain == "twitter":
        data = HashtagDataPreparser(dataset_path)
    elif dataset_domain== "lastfm":
        data = LastfmDataPreparser(dataset_path+"lastfm_nodes.db", dataset_path+"lastfm_edges.db")

    try:
        data.get_all_data()
    except:
        raise

    net_analyzer = BasicNetworkAnalyzer(data)

    if computation_cmd=="basic_stats" or computation_cmd is None:
        net_analyzer.show_basic_stats()
    elif computation_cmd=="random_similarity":
        circlesims, globalsims = net_analyzer.compare_circle_global_similarity("hashtag", num_random_trials=2)
        plotter.plotLinesYY(circlesims, globalsims, "Friends", "Global")
        print "Circle Average", sum(circlesims)/float(len(circlesims))
        print "Global Average", sum(globalsims)/float(len(globalsims))
    elif computation_cmd=="knn_similarity":
        #Compute K-nearest similarity
        KLIMITS = [5, 10]
        for curr_lim in KLIMITS:
            plot_circle, plot_external = net_analyzer.compare_circle_global_knnsimilarity("hashtag", klim=curr_lim)
            plotter.plotLinesYY(plot_circle, plot_external, "Friends", "Global")
            print "K", curr_lim
            print "Circle Average", utils.mean_sd(plot_circle)
            print "Global Average", utils.mean_sd(plot_external)

    elif computation_cmd=="knn_recommender":
        #Compute K-nearest recommender
        KLIMITS = [5,10]
        rec_analyzer = RecommenderAnalyzer(data, max_recs_shown=10, traintest_split=0.7)
        for curr_lim in KLIMITS:
            local_avg=[]
            global_avg=[]
            Ntotal = 5
            for i in range(Ntotal): # randomize because of training-test split.
                plot_circle, plot_external = rec_analyzer.compare_knearest_recommenders("hashtag", klim=curr_lim)
                print "K", curr_lim
                #print plot_circle, plot_external
                curr_avg_local = utils.mean_sd(plot_circle)
                curr_avg_global =  utils.mean_sd(plot_external)
                print "Circle Average", curr_avg_local
                print "Global Average", curr_avg_global
                local_avg.append(curr_avg_local[0])
                global_avg.append(curr_avg_global[0])
                #plotLinesYY(plot_circle, plot_external, "Friends", "Global")
            print "Local", sum(local_avg)/float(Ntotal)
            print "Global", sum(global_avg)/float(Ntotal)
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

    else:
        assert False, "Bad computation parameter"
    """
