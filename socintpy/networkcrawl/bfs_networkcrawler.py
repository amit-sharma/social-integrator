"""An implementation of a network crawler, using BFS.

"""

from socintpy.networkcrawl.crawler_interface import NetworkCrawlerInterface
from socintpy.util.priority_queue import PriorityQueue
from socintpy.store.graph_buffer import GraphBuffer
from socintpy.store.generic_store import GenericStore
import itertools
import logging
import gc

# BFSnetworkcrawler is called for a specific network, which is required as a
# parameter
class BFSNetworkCrawler(NetworkCrawlerInterface):
    """
    BFS Crawler. Uses a weighted BFS implementation which gives more weight to nodes that are seen multiple times.
    """

    def __init__(self, network, store_type="gml"):
        """ Function to initialize a network crawler.
          Args:
            network: the network object to be crawled
            store_type: the type of storage. Valid values are "gml", "basic_shelve"
                        or "couchdb"
          Returns:
            An initialized crawler object
        """
        self.network = network
        store_class = GenericStore.get_store_class(store_type)
        # Buffer to store network data
        self.gbuffer = GraphBuffer(self.network.label, store_class)
        #self.recover = recover
        # Priority queue in memory that keeps track of nodes to visit
        self.pqueue = PriorityQueue(store_class, store_name=self.network.label +
                                                            "_state")

    """
    def set_seed_nodes(self, seed_values):
      for seed in seed_values:
        self.add_to_crawl(seed)
    """

    def add_to_crawl(self, node, priority=0):
        self.pqueue.push(node, priority)
        #self.visited[node] = False


    def __del__(self):
        self.close()

    def close(self):
        self.gbuffer.close()
        self.pqueue.close()

    def crawl(self, seed_nodes, max_nodes=10, recover=True, checkpoint_frequency=100):
        """ Function to crawl the network using BFS.
            Works in two modes: fresh or recover. If recover is False, then
    seed_nodes needs to be not None.
        """
        # if the crawl was stopped for some reason, recover parameter helps to
        # restart it from where it stopped.
        #if self.recover and not self.pqueue.is_empty():
        # Just trying to minimize conflict. May lead to duplicate storage.
        #  node_counter = itertools.count(self.gbuffer.nodes_store.get_maximum_id()+1)
        #  edge_counter = itertools.count(self.gbuffer.edges_store.get_maximum_id()+1)
        #  print("Node counter", node_counter)
        #else:
        #  self.set_seed_nodes(self.seed_nodes)

        # WARNING: The initialize function assumes that the checkpoint_freq remains the same between current run and the previous run which is being recovered.
        prior_num_nodes_visited, prior_num_edges_visited = self.pqueue.initialize(seed_nodes, do_recovery=recover,
                                                                                  checkpoint_freq=checkpoint_frequency)
        node_counter = itertools.count(prior_num_nodes_visited)
        edge_counter = itertools.count(prior_num_edges_visited)
        iterations = 0
        try:
            while (not self.pqueue.is_empty()) and iterations < max_nodes:
                iterations += 1
                new_node = self.pqueue.pop()
                logging.info("Popped %s from queue and now starting to process it..." % new_node)
                print "Popped %s from queue and now starting to process it..." % new_node
                # Ignore if new_node has already been visited
                if new_node in self.pqueue.visited:
                    continue


                # Get details about the current node
                new_node_info = self.network.get_node_info(new_node)
                if new_node_info is None:
                    error_msg = "Crawler: Error in fetching node info. Skipping node %s" % new_node
                    logging.error(error_msg)
                    print error_msg
                    continue
                    #raise NameError
                # Assume dict output for all stores.
                new_node_edges_info = self.network.get_edges_info(new_node)
                if new_node_edges_info is None:
                    error_msg = "Crawler: Error in fetching node edges info. Skipping node %s" % new_node
                    logging.error(error_msg)
                    print error_msg
                    continue
                    #raise NameError

                logging.info("Crawler: Got information about %s" % new_node)

                print "Got all data"
                for edge_info in new_node_edges_info:
                    node = edge_info['target']

                    if node not in self.pqueue.visited:
                        # Update priority if node already exists in the queue
                        if node in self.pqueue.queue_dict:
                            node_priority = self.pqueue.mark_removed(node)
                            updated_priority = node_priority - 1
                            self.add_to_crawl(node, updated_priority)
                        # Otherwise just add the node to the queue
                        else:
                            self.add_to_crawl(node)

                # Now storing the data about the node and its edges
                print "Starting to store node info"
                new_node_info['id'] = next(node_counter)

                self.gbuffer.store_node(new_node, new_node_info)
                self.pqueue.mark_visited(new_node)
                for edge_info in new_node_edges_info:
                    edge_info['id'] = next(edge_counter)
                    self.gbuffer.store_edge(str(edge_info['id']), edge_info)
                logging.info("Processed %s \n" % new_node)
                print "Processed ", new_node
                if iterations % checkpoint_frequency == 0:
                    #curr_num_nodes_stored = self.gbuffer.nodes_store.get_maximum_id() + 1
                    curr_num_nodes_stored = self.gbuffer.nodes_store.get_count_records()
                    print("Making a checkpoint for pqueue state", iterations, curr_num_nodes_stored)
                    logging.info("Making a checkpoint for pqueue state: %d", curr_num_nodes_stored)
                    self.pqueue.save_checkpoint(curr_num_nodes_stored, self.gbuffer.edges_store.get_count_records(),
                                                checkpoint_frequency)
                    #print gc.collect()
        except (KeyboardInterrupt, SystemExit):
            self.pqueue.close()
            self.gbuffer.close()
        return
