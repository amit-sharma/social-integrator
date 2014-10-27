import matplotlib
matplotlib.use('Agg')
from datetime import datetime
from collections import defaultdict

import math
import matplotlib.pyplot as plt
import os
import pydot
import time
import numpy as np


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
                    interactions.append((datetime.fromtimestamp(item_tuple[1]), v, num_friends))
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
        graph.write_pdf(filename, prog=['neato', '-s', '-n'])

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

    def plot_item_adoption_by_friend_adoption(self, timestep, bin_size, directory, ignore_zero=False):
        bins = defaultdict(list)
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True, should_have_friends=True):
            num_friends = len(v.get_friend_ids())
            if num_friends > 0 or not ignore_zero:
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
            plt.hist(counts,bins=max(1, max(counts)), log=True)
            plt.title(str(bin) + " to " + str(bin+bin_size-1) + " friends")
            plt.savefig(directory + '/' + str(bin) + '.png')
            plt.close(fig)

    def plot_num_interactions_vs_num_friends(self, filename):
        x = []
        y = []
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True,should_have_friends=True):
            num_interactions = len(v.get_items_interacted_with(self.interact_type))
            num_friends = len(v.get_friend_ids())
            x.append(num_friends)
            y.append(num_interactions)
        fig = plt.figure()
        poly = np.poly1d(np.polyfit(x,y,1))
        print np.polyfit(x,y,1)
        spacing = np.linspace(0,max(x),10000)
        plt.xscale('log')
        plt.yscale('log')
        plt.plot(x, y, '.', label="data")
        plt.plot(spacing, poly(spacing), '-', label=("y = {:.2f}x+{:.2f}".format(poly.c[0], poly.c[1])), color='red')
        plt.title("number of friends vs number of interactions, r^2 = %.4f" % self._r2(x, y))
        plt.ylabel('interactions')
        plt.xlabel('friends')
        plt.legend(loc='upper right')
        plt.xlim([0, max(x)])
        plt.ylim([-1, max(y)])
        plt.savefig(filename)
        plt.close(fig)

    # Calculates the proportion (alpha) of item adoptions that occurred after a friend adopted (within timestep)
    # alpha is measured for items after they have existed for alpha_lifetime time
    # alpha is then compared to the popularity of the item (total adoptions) after popularity_lifetime time
    def plot_popularity_vs_alpha(self, timestep, alpha_lifetime, popularity_lifetime, filename, ignore_zero_friends=False):
        start = time.time()
        interactions_by_item = defaultdict(list)
        now = datetime.now()
        max_interaction_time = datetime(year=1970,month=1,day=1)
        for v in self.netdata.get_nodes_iterable(should_have_interactions=True, should_have_friends=True):
            interactions = v.get_items_interacted_with(self.interact_type, return_timestamp=True)
            for item in interactions:
                date = (datetime.fromtimestamp(item[1]))
                max_interaction_time = max(date, max_interaction_time)
                interactions_by_item[item[0]].append((date, v.uid))
        popularities = []
        alphas = []
        for item, interactions in interactions_by_item.iteritems():
            interactions.sort()
            # item has existed long enough
            if interactions[0][0] + max(popularity_lifetime, alpha_lifetime) <= max_interaction_time:
                max_popularity_time = interactions[0][0] + popularity_lifetime
                max_alpha_time = interactions[0][0] + alpha_lifetime
                friend_adoptions = 0
                independent_adoptions = 0
                prev_adoptions = []
                for i in interactions:
                    i_time = i[0]
                    node = self.netdata.get_node_objs([i[1]])[0]
                    friends = node.get_friend_ids()
                    if i_time <= max_alpha_time and len(friends) > 0 or not ignore_zero_friends:
                        friend_adopted = False
                        for a in prev_adoptions:
                            if a[0] + timestep >= i_time and a[1] in friends:
                                friend_adopted = True
                        if friend_adopted:
                            friend_adoptions += 1
                        else:
                            independent_adoptions += 1
                        prev_adoptions.append(i)
                if friend_adoptions + independent_adoptions > 0:
                    alpha = friend_adoptions / float(friend_adoptions + independent_adoptions)
                    popularity = len([i for i in interactions if i[0] <= max_popularity_time])
                    popularities.append(popularity)
                    alphas.append(alpha)
        fig = plt.figure()
        plt.plot(alphas, popularities, '.', label="data")
        poly = np.poly1d(np.polyfit(alphas, popularities, 1))
        spacing = np.linspace(0,max(alphas),1000)
        plt.plot(spacing, poly(spacing), '-', label=("y = {:.2f}x+{:.2f}".format(poly.c[0], poly.c[1])), color='red')
        plt.yscale('log')
        plt.title("alpha vs popularity, r^2 = %.4f" % self._r2(alphas, popularities))
        plt.ylabel('popularity')
        plt.xlabel('alpha')
        plt.legend(loc='upper right')
        plt.ylim([-1, max(popularities)+5])
        plt.xlim([-0.1, 1.1])
        plt.savefig(filename)
        plt.close(fig)
        print time.time() - start

    def _r2(self, x, y, degree=1):
        poly = np.poly1d(np.polyfit(x, y, degree))
        yhat = poly(x)
        ybar = np.sum(yhat)/len(y)
        ssreg = np.sum((yhat-ybar)**2)
        sstot = np.sum((y-ybar)**2)
        return ssreg / sstot


