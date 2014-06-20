import matplotlib.pyplot
import networkx as nx


class NetworkVisualizor(object):
    def __init__(self, networkdata):
        self.netdata = networkdata
        self.graph = nx.Graph()

    def draw_network(self):
        for v in self.netdata.get_nodes_iterable(should_have_friends=True):
            self.graph.add_node(v.uid)
            for fr_id in v.get_friend_ids():
                self.graph.add_edge(v.uid, fr_id)
        nx.draw(self.graph)#, pos=nx.spring_layout(self.graph))
