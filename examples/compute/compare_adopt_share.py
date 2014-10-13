from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
import numpy as np
import math
from collections import defaultdict
import  operator
import socintpy.util.plotter as plotter

def get_meanified_array(arr, binwidth, method="mean"):
    if method=="mean":
        return [np.mean(arr[i:min(i+binwidth, len(arr))]) for i in range(0,len(arr), binwidth)]
    elif method=="median":
        return [np.median(arr[i:min(i+binwidth, len(arr))]) for i in range(0,len(arr), binwidth)]

def compare_meanified_array(items_pop1, items_pop2):
    arr1=[];arr2=[]
    for k, v in sorted(items_pop1.iteritems(), key=operator.itemgetter(1)):
        if v >=10:
            arr1.append(v)
            if k in items_pop2:
                arr2.append(items_pop2[k])
            else:
                arr2.append(0)
    plotter.plotLinesYY(get_meanified_array(arr1, 100, method="median"),get_meanified_array(arr2, 100, method="median"),
                        "listen", "love", labelx="Items", 
                        labely="Number of people who interact with this item",
                        display=True, logyscale=True)
    plotter.plotLinesYY(get_meanified_array(arr1, 100, method="mean"),get_meanified_array(arr2, 100, method="mean"),
                        "listen", "love", labelx="Items", 
                        labely="Number of people who interact with this item",
                        display=True, logyscale=True)
    return

class AdoptShareComparer(BasicNetworkAnalyzer):
    def compare(self):
        x= self.get_sum_interactions_by_type()
        return x

    def get_items_popularity2(self, interact_type, count_duplicates):
        items_pop =  defaultdict(int)
        for node in self.netdata.get_nodes_iterable(should_have_interactions=True):
            interacts = node.get_items_interacted_with(interact_type,
                            return_timestamp=count_duplicates)
            for item in interacts:
                if count_duplicates:
                    item_id = item[0]
                else:
                    item_id = item
                items_pop[item_id] += 1

        return items_pop

    def compare_interaction_types2(self, min_exposure=1, return_duplicates=True):
        num_interacts = {}
        for key in self.netdata.interact_types_dict.keys():
            num_interacts[key] = []
        for node in self.netdata.get_nodes_iterable(should_have_interactions=True):
            for key, interact_type in self.netdata.interact_types_dict.iteritems():
                if key=="listen":
                    items = node.get_items_interacted_with(interact_type, 
                                                       return_timestamp=True)
                    num_items = len(self.get_high_freq_items2(items, min_exposure,
                                                              return_duplicates))
                else:
                    items = node.get_items_interacted_with(interact_type, 
                                                       return_timestamp=return_duplicates)
                    num_items = len(items)
                num_interacts[key].append(num_items)
#for arr in num_interacts.itervalues():
#            arr.sort()
        return num_interacts

    def get_high_freq_items2(self, items_set, min_freq, return_duplicates):
        freq_items = {}
        for item in items_set:
            item_id = item[0]
            if item_id not in freq_items:
                freq_items[item_id] = 0
            freq_items[item_id] += 1
        eligible_items = [key for key, value in freq_items.iteritems() if value >=min_freq]
        if return_duplicates:
            return [item_id for item_id, timestamp in items_set if item_id in eligible_items]
        else:
            return eligible_items

def compare_interact_types_byuser(na, itype1, itype2, binwidth, duplicates=True, min_exposure=1, 
                                  plot_type="xy", logyscale=False, logxscale=False):
    y = na.compare_interaction_types2(min_exposure=min_exposure, return_duplicates=duplicates)
    tuple_arr = [(y[itype1][i], y[itype2][i]) for i in range(len(y[itype1])) if y[itype1][i]>0]
    itype1_arr = [val[0] for val in tuple_arr]
    itype2_arr = [val[1] for val in tuple_arr] 
    print max(itype1_arr), max(itype2_arr)

    itype1_arr2 = get_meanified_array(itype1_arr,binwidth)
    itype2_arr2 = get_meanified_array(itype2_arr,binwidth)

    labely_part = "Interactions" if duplicates else "Artists"
    if plot_type=="yy":
        plotter.plotLinesYY(itype1_arr2, itype2_arr2, itype1, itype2, labelx="Users (sorted by listen)",
                            labely="Number of "+ labely_part,
                            display=True, logyscale=False)
    elif plot_type=="xy":
        """
        plotter.plotLinesXY(itype1_arr, itype2_arr, labelx=itype1,
                            labely=itype2, title_str="Number of "+ labely_part,
                            display=True, logyscale=logyscale, logxscale=logxscale,
                            ylim_val=[0, 100000])
        """
        import matplotlib.pyplot as plt; plt.loglog(itype1_arr, itype2_arr, 'o')
        plt.show()
    return itype1_arr, itype2_arr

def compare_items_rank(items_pop1, items_pop2, topk=1000):
    arr1=[];arr2=[]
    rankdiff = []; rank=[]
    arr1 = sorted(items_pop1.iteritems(), key=operator.itemgetter(1), reverse=True)
    arr2 = sorted(items_pop2.iteritems(), key=operator.itemgetter(1), reverse=True)
    rankdict1 = {}; rankdict2 = {}
    i=1
    for k, v in arr1:
        rankdict1[k] = i
        i+=1
    i=1
    for k, v in arr2:
        rankdict2[k] = i
        i+=1
    count = 0;
    t1=[];t2=[]
    for k,v in arr1[:topk]:
        if k in rankdict2:
            rankdiff.append(abs(rankdict1[k] -rankdict2[k]))#/float(rankdict1[k]))
            rank.append(rankdict1[k])
            t1.append(rankdict1[k]); t2.append(rankdict2[k])
            if rankdict2[k] >topk*2:
                count += 1
        else:
            print "This rank is not even listed in top", topk, "loved artists", rankdict1[k]
    print count
    plotter.plotLinesXY(t1, t2, "listen", "love", ylim_val=[0,5000])
    return max(rankdiff), np.mean(rankdiff), np.median(rankdiff), min(rankdiff)

if __name__ == "__main__":
    na = AdoptShareComparer(data)
    x = na.get_sum_interactions_by_type()
    listen_arr, love_arr = compare_interact_types_byuser(na, "listen", "love", binwidth=100, 
                                  duplicates=False, min_exposure=2, logyscale=False,
                                  logxscale=True, plot_type="xy")

    promisc = [(listen_arr[i], float(love_arr[i])/listen_arr[i]) for i in range(len(listen_arr))]
    plotter.plotHist([val2 for val1, val2 in promisc if val2<=3], 
                     labelx="Ratio of love to listen interactions for a user", 
                     labely="Frequency of Users", xlim_val=[0,3])
    listen_dict = {}
    for val1, val2 in promisc:
        listen_dict[val1] = listen_dict.get(val1, [])
        listen_dict[val1].append(val2)
        
    mean_promisc = []
    listen_values = []
    for key, val_arr in listen_dict.iteritems():
        listen_values.append(key)
        mean_promisc.append(np.mean(val_arr))
    plotter.plotLinesXY(listen_values, mean_promisc, labelx="Number of listen interactions",
                        labely="Ratio of love/listen interactions",
                        display=True, logxscale=False, logyscale=False)
       
    items_pop_listen = na.get_items_popularity2(interact_type=na.netdata.interact_types_dict["listen"], count_duplicates=True)
    items_pop_love = na.get_items_popularity2(interact_type=na.netdata.interact_types_dict["love"], count_duplicates=True)
    plotter.plotHist(items_pop_listen.values(), labelx="Total number of Listens for an item", 
                     labely="Frequency")
    plotter.plotHist(items_pop_love.values(), labelx="Total number of Loves for an item", 
                     labely="Frequency")
    sorted_items_listen = sorted(items_pop_listen.iteritems(), key=operator.itemgetter(1), reverse=True)
    sorted_items_love = sorted(items_pop_love.iteritems(), key=operator.itemgetter(1), reverse=True)
    # next do rank alignment--like ndcg
    
    items_pop_listen = na.get_items_popularity2(interact_type=na.netdata.interact_types_dict["listen"], count_duplicates=False)
    items_pop_love = na.get_items_popularity2(interact_type=na.netdata.interact_types_dict["love"], count_duplicates=False)
    compare_items_rank(items_pop_listen, items_pop_love)
    
    compare_meanified_arrays(items_pop_listen, items_pop_love)
