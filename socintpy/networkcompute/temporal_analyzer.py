from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
import operator
from collections import defaultdict
import socintpy.util.plotter as plotter

class TemporalAnalyzer(BasicNetworkAnalyzer):
    def __init__(self, netdata):
        BasicNetworkAnalyzer.__init__(self, netdata)
        self.interactions_stream = []
        self.items_pop = defaultdict(int)
        self.num_users_with_interactions = 0

    def create_interactions_stream(self, interact_type):
        for node in self.netdata.get_nodes_iterable(should_have_interactions=True):
            node_interacts = node.get_items_interacted_with(interact_type, return_timestamp=True)
            new_arr = [(node, item_id, timestamp) for item_id, timestamp in node_interacts] 
            self.interactions_stream.extend(new_arr)
            for item_id, t in node_interacts:
                self.items_pop[item_id] += 1
            self.num_users_with_interactions += 1
        print [(node, item_id, timestamp) for node, item_id, timestamp in self.interactions_stream[1:10]]
        self.interactions_stream = sorted(self.interactions_stream, key=operator.itemgetter(2))
        self.show_basic_temporal_stats()
        return 

    def find_early_late_adopters(self, interact_type):
        item_interactions = defaultdict(int)
        num_interactions = defaultdict(int)
        visited = defaultdict(bool)
        for node, item_id, timestamp in self.interactions_stream:
            if self.items_pop[item_id] >=0 and not visited[(node.uid,item_id)]:
                #percent_users_before = round(item_interactions[item_id]/float(self.items_pop[item_id]), ndigits=2)
                percent_users_before = round(item_interactions[item_id]/float(self.num_users_with_interactions), ndigits=2)
                #percent_users_before = item_interactions[item_id]
                num_interactions[percent_users_before] += 1
                item_interactions[item_id] +=  1
            visited[(node.uid, item_id)]=True
        return num_interactions
            
    def count_friends_before_interaction(self, interact_type):
        item_interactions = defaultdict(list)
        num_interactions = defaultdict(int)
        visited = defaultdict(bool)
        for node, item_id, timestamp in self.interactions_stream:
            if self.items_pop[item_id] >=0 and not visited[(node.uid,item_id)]:
                #percent_users_before = round(item_interactions[item_id]/float(self.items_pop[item_id]), ndigits=2)
                percent_users_before = sum([1 if fid in item_interactions[item_id] else 0 for fid in node.get_friend_ids()])
                #percent_users_before = item_interactions[item_id]
                num_interactions[percent_users_before] += 1
                item_interactions[item_id].append(node.uid)
            visited[(node.uid, item_id)]=True
        return num_interactions
    
    def compute_probability_friends_influence(self, interact_type):
        count_influence = defaultdict(int)
        count_total = defaultdict(int)
        for node in self.netdata.get_nodes_iterable(should_have_friends=True):
            items_circle_pop = node.compute_items_circle_popularity()
            node_interacts = node.get_items_interacted_with(interact_type)
            for item_id, circle_pop in items_circle_pop.iteritems():
                pass 

    def plot_early_late_adoption(self, interact_type):
        num_interacts = self.find_early_late_adopters(interact_type)
        prev_users = []; freq=[]
        for k,v in sorted(num_interacts.iteritems(), key=operator.itemgetter(0)):
            prev_users.append(k)
            freq.append(v)
        plotter.plotLinesXY(prev_users, freq, labelx="Fraction of users who have interacted before with an item", 
                labely="Number of interactions", logyscale=True)
    
    def get_max_min_time(self, interact_type):
        print "First interaction", self.interactions_stream[0][2]
        print "Last interaction", self.interactions_stream[-1][2]
