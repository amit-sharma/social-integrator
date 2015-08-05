from datetime import datetime, timedelta
from collections import defaultdict
from socintpy.cythoncode.cnetwork_node import compute_node_similarity_pywrapper

import random
import matplotlib
import csv
import math
import matplotlib.pyplot as plt
import os
import pydot
import random
import time
import numpy as np
import networkx as nx


MAX_NODE_SIZE = 2.0


class NetworkVisualizer(object):
    def __init__(self, networkdata):
        self.netdata = networkdata
        self.graph = None
        self.interact_type = self.netdata.interaction_types[0]

    def find_most_interacted_items(self, interact_type, min_interactions):
        items = {}
        for v in self.netdata.get_nodes_iterable(
            should_have_interactions=True,
            should_have_friends=True
        ):
            for item_tuple in v.get_items_interacted_with(
                interact_type=interact_type,
                return_timestamp=True
            ):
                count = items.get(item_tuple[0], 0) + 1
                items[item_tuple[0]] = count
        return [(v, k) for k, v in items.iteritems() if v >= min_interactions]

    def get_items_interactions(
        self,
        interact_type,
        items,
        should_have_friends=False
    ):
        items_interactions = {}
        for v in self.netdata.get_nodes_iterable(
            should_have_interactions=True,
            should_have_friends=should_have_friends
        ):
            for item_tuple in v.get_items_interacted_with(
                interact_type=interact_type,
                return_timestamp=True
            ):
                if item_tuple[0] in items:
                    interactions = items_interactions.get(
                        items_interactions[item_tupl[0]],
                        []
                    )
                    interact_time = datetime.strptime(
                        item_tuple[1],
                        "%m/%d/%Y %I:%M:%S %p"
                    )
                    interactions.append((v.uid, interact_time))
                    items_interactions[item_tuple[0]] = interactions
        return items_interactions

    # Creates an adoption cascade for the give item_id, and writes a ps to the
    # filename of the resulting graph
    def plot_item_cascade(
        self,
        interact_type,
        item_id,
        timestep,
        filename,
        max_time_distance=1
    ):
        interactions = []
        max_friends = 0
        for v in self.netdata.get_nodes_iterable(
            should_have_interactions=True,
            should_have_friends=True
        ):
            items = v.get_items_interacted_with(
                interact_type=interact_type,
                return_timestamp=True
            )
            for item_tuple in items:
                if item_id == item_tuple[0]:
                    num_friends = len(v.get_friend_ids())
                    max_friends = max(num_friends, max_friends)
                    interactions.append(
                        (datetime.fromtimestamp(item_tuple[1]), v, num_friends)
                    )
        interactions = sorted(interactions)

        graph = pydot.Dot(graph_type='digraph')
        graph.set_node_defaults(
            fontsize=60,
            shape='circle',
            label='\"\"',
            style='filled',
            fillcolor='black',
            pin='true'
        )
        graph.set_graph_defaults(
            outputorder='nodesfirst',
            labelloc='t',
            fontsize=100
        )
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
            while index < len(interactions) and \
                    interactions[index][0] <= max_date:
                interaction = interactions[index]
                level.append(interaction[1])
                index += 1
                width = max(
                    0.1,
                    math.log(interaction[2]+1, 2) * MAX_NODE_SIZE /
                    math.log(max_friends+1, 2)
                )
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
                        edges.add(
                            (min(node2.uid, node.uid), max(node2.uid, node.uid))
                        )

        # set up graph meta-data
        num_edges = len(edges)
        num_nodes = sum([len(level) for level in levels])
        label = '# Nodes: %(nodes)d, # Edges: %(edges)d' % \
            {'nodes': num_nodes, 'edges': num_edges}
        graph.set_label(label)

        # lay out the nodes
        output = graph.create_dot()

        # read in the new graph to lay out the edges
        graph = pydot.graph_from_dot_data(output)
        for edge in edges:
            graph.add_edge(pydot.Edge(edge[0], edge[1]))
        graph.write_pdf(filename, prog=['neato', '-s', '-n'])

    # histogram of users by number of friends
    def plot_item_by_connectedness(
        self,
        interact_type,
        item_id,
        filename,
        num_bins=100
    ):
        data = []
        for v in self.netdata.get_nodes_iterable(
            should_have_interactions=True,
            should_have_friends=True
        ):
            items = v.get_items_interacted_with(
                interact_type,
                return_timestamp=True
            )
            for item_tuple in items:
                if item_id == item_tuple[0]:
                    num_friends = len(v.get_friend_ids())
                    data.append(num_friends)
        fig = plt.figure()
        plt.hist(data, bins=num_bins, log=True)
        plt.savefig(filename)
        plt.close(fig)


def process_data_for_popularity_prediction(
    netdata,
    interact_type,
    filename,
    t2,
    t1=None,
    k=1,
    use_similarity=False,
    sub_sample=False
):
    '''
    input:
        netdata - netdata object
        interact_type (int)- the type of interactions to process
        filename (string)- csv to save data to
        k - minimum number of interactions (if t1 is none, then early adoption period is first k)
        t1 (timedelta or int) - how long the early adopter period is,
            or how many adopters are considered the early adopter period
        t2 (timedelta or int) - how long final popularity time is
    '''
    t2 = t2.total_seconds()
    if t1 is not None:
        t1 = t1.total_seconds()
    start = time.time()
    interactions_by_item = defaultdict(list)
    # change here
    max_interaction_time = 0
    # get all interactions for all items and put in a dictionary
    # for each item we have all interactions (user id and timestamp)
    total_nodes = netdata.get_total_num_nodes()
    i = 0
    for v in netdata.get_nodes_iterable(
        should_have_interactions=True,
        should_have_friends=True
    ):
        if i % 10000 == 0:
            print i, 'of', total_nodes
        i += 1
        interactions = v.get_items_interacted_with(
            interact_type,
            return_timestamp=True
        )
        for item in interactions:
            date = item[1]
            max_interaction_time = max(date, max_interaction_time)
            interactions_by_item[item[0]].append((date, v.uid))
    with open(filename, 'w') as csvfile:
        writer = csv.writer(csvfile)
        total_items = len(interactions_by_item)
        completed = 0
        # for each item calculate data for prediction
        for item, interactions in interactions_by_item.iteritems():
            if completed % 10000 == 0:
                print completed, "of", total_items
            completed += 1

            # sort by timestamp
            interactions.sort()

            max_t2_time = interactions[0][0] + t2
            # item has existed long enough and has enough interactions fast enough
            if len(interactions) >= k and interactions[k-1][0] <= max_t2_time \
                    and max_t2_time <= max_interaction_time \
                    and (
                            t1 is None or
                            (interactions[k-1][0] <= interactions[0][0] + t1 and (len(interactions) == k or interactions[k][0] > interactions[0][0] + t1))
                        ):
                ran = random.random()
                if ran > 0.01 and sub_sample:
                    continue
                adopters = []
                if t1 is None:
                    max_t1_time = interactions[k-1][0]
                else:
                    max_t1_time = interactions[0][0] + t1
                # get early adopters
                for i in interactions:
                    i_time = i[0]
                    i_node = netdata.get_node_objs([i[1]])[0]
                    if i_time <= max_t1_time:
                        if t1 is not None or len(adopters) < k:
                            adopters.append((i_time, i_node))
                if t1 is None and len(adopters) > k:
                    continue

                # all adoptions before the kth adoption
                prev_adoptions = []

                # list of similarities between early adopters, ignores -1s
                similarities = []

                early_popularity = len(adopters)

                ####### Calculate features #######
                root = adopters[0][1]
                root_time = adopters[0][0]

                # construct subgraph of early adopters
                subgraph = nx.Graph()
                for a in adopters:
                    node = a[1]
                    friends = node.get_friend_ids()
                    subgraph.add_node(node.uid)
                    for a2 in adopters:
                        node2 = a2[1]
                        if node2.uid in friends:
                            subgraph.add_edge(node.uid, node2.uid)
                        pass
                # baseline features
                b_root_outdegree = None
                b_root_age = None
                b_root_activity = None
                b_outdegrees = []
                b_outdegrees_sub = []
                b_network_ages = []
                b_activities = []  # approximate days active, by amount of loves
                b_orig_connections = subgraph.degree(root.uid)
                b_border_nodes = set([])
                b_border_edges = 0
                b_subgraph = len(subgraph.edges())
                b_trees = len(list(nx.connected_component_subgraphs(subgraph)))
                b_depth = 0  # average distance from node to node for connected
                b_time = []
                b_time_first = None  # average time between reshares for 1st 1/2
                b_time_last = None  # average time between reshares for 2nd 1/2

                distances = 0
                num_ns = 0
                for g in nx.connected_component_subgraphs(subgraph):
                    try:
                        distances += nx.average_shortest_path_length(g) * \
                            len(g.nodes())
                    except:
                        pass
                b_depth = distances / len(subgraph.nodes())

                if t1 is not None:
                    adoptions_by_day = np.zeros(t1 / (60*60*24)).astype(int)

                for en, adopter in enumerate(adopters):
                    node = adopter[1]

                    adopt_time = adopter[0]
                    friends = node.get_friend_ids()
                    adopter_interactions = sorted(
                        list(
                            node.get_items_interacted_with(
                                interact_type,
                                return_timestamp=True
                            )
                        ),
                        key=lambda x: x[1]
                    )
                    if t1 is not None:
                        adoptions_by_day[(max_t1_time - adopt_time) / (60*60*24)-1] += 1
                    # similarity
                    if use_similarity:
                        node.create_training_test_sets_bytime(
                            interact_type,
                            max_t1_time,
                            -1
                        )
                        for pa in prev_adoptions:
                            pnode = pa[1]
                            similarity = compute_node_similarity_pywrapper(
                                node=node,
                                other_node=pnode,
                                interact_type=interact_type,
                                data_type_code=ord('c'),
                                min_interactions_per_user=5,
                                time_diff=-1,
                                time_scale=ord('w')
                            )
                            if similarity >= 0:
                                similarities.append(similarity)
                    prev_adoptions.append(adopter)
                    # Baseline (Cheng et al. Features)

                    last_month_interactions = len(
                        [
                            i for i in adopter_interactions
                            if (adopt_time - (60*60*24*14)) <=
                            i[1] <= max_t1_time
                        ]
                    )

                    age = adopt_time - adopter_interactions[0][1]

                    b_border_edges += len(friends)
                    b_border_nodes.update(friends)
                    if en == 0:
                        b_root_age = age
                        b_root_outdegree = len(friends)
                        b_root_activity = last_month_interactions
                    else:
                        b_outdegrees.append(len(friends))
                        b_outdegrees_sub.append(subgraph.degree(node.uid))
                        b_network_ages.append(age)
                        b_activities.append(last_month_interactions)
                        b_time.append(adopt_time-root_time)

                split = len(adopters) / 2
                div = split if split > 0 else 1
                b_time_first = (
                    adopters[split-1][0] - adopters[0][0]
                ) / div
                b_time_last = (
                    adopters[-1][0] - adopters[split][0]
                ) / div

                if early_popularity >= 1:
                    row = ()
                    final_popularity = len(
                        [i for i in interactions if i[0] <= max_t2_time]
                    )
                    if use_similarity:
                        if len(similarities) == 0:
                            sim_attrs = (0, 0, 0, 0)
                        else:
                            sim_attrs = (
                                len(similarities),  # sim
                                np.mean(similarities),  # sim
                                np.median(similarities),  # sim
                                np.max(similarities)  # sim
                            )
                        row = row + sim_attrs
                    row = row + (
                        b_root_outdegree,  # outdeg v0 - root
                        b_root_age,  # fb_age0 - root
                        b_root_activity,  # activity0 - root
                    )
                    if early_popularity > 1:
                        row = row + (
                            np.mean(b_outdegrees),  # friends_avg - reshare
                            np.mean(b_network_ages),  # fb_ages_avg - reshare
                            np.mean(b_activities),  # activities_avg - reshare
                            np.mean(b_outdegrees_sub),  # avg on subgraph - struct
                        )
                    else:
                        row = row + (0, 0, 0, 0)

                    row = row + (
                        b_orig_connections,  # orig_connections - struct
                        len(b_border_nodes),  # border_nodes - struct
                        b_border_edges,  # border edges - struct
                        b_subgraph,  # subgraph' - struct
                        b_trees,  # approximate of depth - struct
                        b_depth,  # approximate of depth - struct
                    )
                    # if we are using k, then use these features
                    if k > 1:
                        row = row + tuple(b_outdegrees)[:k-1] + \
                            tuple(b_outdegrees_sub)[:k-1] + \
                            tuple(b_time)[:k-1]
                    row = row + (
                        b_time_first,  # time'_1...k/2 - temporal
                        b_time_last,  # time'_k/2...k - temporal
                    )
                    if t1 is not None:
                        row = row + tuple(adoptions_by_day)  # ordinal
                    # ordinal and label
                    row = row + (early_popularity, final_popularity, )
                    writer.writerow(row)

    print time.time() - start
