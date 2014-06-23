import matplotlib.pyplot
import networkx as nx
from datetime import datetime

class NetworkVisualizor(object):
    def __init__(self, networkdata):
        self.netdata = networkdata
        self.graph = None
        self.interact_type = self.netdata.interaction_types[0] 
        self.create_network()

    def create_network(self):
        self.graph = nx.Graph()
        for v in self.netdata.get_nodes_iterable(should_have_friends=True):
            self.graph.add_node(v.uid)
            for fr_id in v.get_friend_ids():
                self.graph.add_edge(v.uid, fr_id)
        return self.graph

    def draw_network(self):
        nx.draw(self.graph)#, pos=nx.spring_layout(self.graph))
    
    def plot_item_adoption(self, item_id):
        item_interactions = {}
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True):
            item_tuple = v.get_items_interacted_with(self.interact_type, return_timestamp = True)
            for curr_itemid, timestamp in item_tuple:
                if item_id == curr_itemid:
                    try:
                        interact_time = datetime.strptime(timestamp, "%m/%d/%Y %I:%M:%S %p")
                        item_interactions[interact_time] = v.uid
                    except ValueError, ve:
                        print timestamp
                        print ve

        sorted_interacts = sorted(item_interactions.items(), key=lambda x: x[0])
        interact_nodes =[]
        plot_data = []
        for timestamp,uid in sorted_interacts:
            min_dist = len(self.graph)
            for node_id in interact_nodes:
                try:
                    dist = nx.shortest_path_length(self.graph, node_id, uid)
                    if dist < min_dist:
                        min_dist = dist
                except nx.NetworkXNoPath:
                    pass
            if len(interact_nodes) > 0:
                plot_data.append((timestamp, min_dist))

            interact_nodes.append(uid)
        return plot_data
