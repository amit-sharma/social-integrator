import matplotlib
matplotlib.use('Agg')
from datetime import datetime
from collections import defaultdict

import math
import matplotlib.pyplot as plt
import os
import pydot


MAX_NODE_SIZE = 2.0


class NetworkVisualizer(object):
    def __init__(self, networkdata):
        self.netdata = networkdata
        self.graph = None
        self.interact_type = self.netdata.interaction_types[0]

    def find_most_interacted_items(self, min_interactions):
        items = {}
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True, should_have_friends=True):
            for item_tuple in v.get_items_interacted_with(self.interact_type, return_timestamp=True):
                count = items.get(item_tuple[0],0) + 1
                items[item_tuple[0]] = count
        return [(v, k) for k,v in items.iteritems() if v >= min_interactions]

    def get_items_interactions(items, should_have_friends=False):
        items_interactions = {}
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True, should_have_friends=should_have_friends):
            for item_tuple in v.get_items_interacted_with(self.interact_type, return_timestamp=True):
                if item_tuple[0] in items:
                    interactions = items_interactions.get(items_interactions[item_tupl[0]], [])
                    interact_time = datetime.strptime(item_tuple[1], "%m/%d/%Y %I:%M:%S %p")
                    interactions.append((v.uid, interact_time))
                    items_interactions[item_tuple[0]] = interactions
        return items_interactions

    # Creates an adoption cascade for the give item_id, and writes a ps to the filename of the resulting graph
    def plot_item_cascade(self, item_id, timestep, filename, max_time_distance=1):
        interactions = []
        max_friends = 0
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True, should_have_friends=True):
            items = v.get_items_interacted_with(self.interact_type, return_timestamp=True)
            for item_tuple in items:
                if item_id == item_tuple[0]:
                    num_friends = len(v.get_friend_ids())
                    max_friends = max(num_friends, max_friends)
                    interactions.append((datetime.strptime(item_tuple[1], "%m/%d/%Y %I:%M:%S %p"), v, num_friends))
        interactions = sorted(interactions)

        graph = pydot.Dot(graph_type='digraph')
        graph.set_node_defaults(fontsize=60, shape='circle', label='\"\"', style='filled', fillcolor='black', pin='true')
        graph.set_graph_defaults(outputorder='nodesfirst', labelloc='t', fontsize=100)
        graph.set_edge_defaults(color='red', dir='none')

        # Create graph levels
        min_date = interactions[0][0]
        max_date = min_date + timestep
        end_date = interactions[-1][0]
        index = 0
        i = 0
        levels = []
        while min_date <= end_date:
            level = []
            subgraph = pydot.Subgraph()
            subgraph.set_rank('same')
            placeholder = pydot.Node('temp' + str(i))
            placeholder.set_label(str(i))
            placeholder.set_color('white')
            placeholder.set_fillcolor('white')
            subgraph.add_node(placeholder)
            if i > 0:
                edge = pydot.Edge('temp' + str(i-1), 'temp' + str(i))
                edge.set_color('white')
                graph.add_edge(edge)
            i += 1
            while index < len(interactions) and interactions[index][0] <= max_date:
                interaction = interactions[index]
                level.append(interaction[1])
                index += 1
                width = max(0.1, (math.log(interaction[2]+1, 2) * MAX_NODE_SIZE / math.log(max_friends+1, 2)))
                node = pydot.Node(str(interaction[1].uid))
                node.set_width(width)
                subgraph.add_node(node)
            graph.add_subgraph(subgraph)
            levels.append(level)
            min_date = max_date
            max_date += timestep

        # get edges
        edges = set([])
        for i, level in enumerate(levels):
            potentials = []
            for p in levels[i:i+1+max_time_distance]:
                potentials.extend(p)
            for node in level:
                friends = node.get_friend_ids()
                for node2 in potentials:
                    if node2.uid in friends:
                        edges.add((min(node2.uid,node.uid),max(node2.uid,node.uid)))

        #set up graph meta-data
        num_edges = len(edges)
        num_nodes = sum([len(level) for level in levels])
        label = '# Nodes: %(nodes)d, # Edges: %(edges)d' % { 'nodes': num_nodes, 'edges': num_edges }
        graph.set_label(label)

        # lay out the nodes
        output = graph.create_dot()

        # read in the new graph to lay out the edges
        graph = pydot.graph_from_dot_data(output)
        for edge in edges:
            graph.add_edge(pydot.Edge(edge[0], edge[1]))
        graph.write_ps(filename, prog=['neato', '-s', '-n'])

    # histogram of users by number of friends
    def plot_item_by_connectedness(self, item_id, filename, num_bins=100):
        data = []
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True, should_have_friends=True):
            items = v.get_items_interacted_with(self.interact_type, return_timestamp=True)
            for item_tuple in items:
                if item_id == item_tuple[0]:
                    num_friends = len(v.get_friend_ids())
                    data.append(num_friends)
        fig = plt.figure()
        plt.hist(data,bins=num_bins,log=True)
        plt.savefig(filename)
        plt.close(fig)

    def plot_item_adoption_by_friend_adoption(self, timestep, bin_size, directory):
        bins = defaultdict(list)
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True, should_have_friends=True):
            num_friends = len(v.get_friend_ids())
            bin = (num_friends/bin_size)*bin_size
            friend_interactions = {}
            for f in self.netdata.get_friends_iterable(v):
                friend_interactions[f.uid] = f.get_items_interacted_with(
                    self.interact_type,
                    return_timestamp=True,
                )
            interactions = defaultdict(list)
            for fid, inters in friend_interactions.iteritems():
                for key, value in inters:
                    interactions[key].append((fid, datetime.fromtimestamp(value)))
            for item in v.get_items_interacted_with(self.interact_type, return_timestamp=True):
                item_interactions = interactions[item[0]]
                item_time = datetime.fromtimestamp(item[1])
                count = 0
                for f, t in item_interactions:
                    if t < item_time and t + timestep >= item_time:
                        count += 1
                bins[bin].append(count)
        if not os.path.exists(directory):
            os.makedirs(directory)
        for bin, counts in bins.iteritems():
            fig = plt.figure()
            plt.hist(counts,bins=max(counts), log=True)
            plt.savefig(directory + '/' + str(bin) + '.png')
            plt.close(fig)

