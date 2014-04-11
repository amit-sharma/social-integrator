## An implementation of a network crawler, using BFS.

from socintpy.networkcrawl.crawler_interface import NetworkCrawlerInterface
from socintpy.util.priority_queue import PriorityQueue
from socintpy.store.graph_buffer import GraphBuffer
from socintpy.store.generic_store import GenericStore
import itertools
import logging

# BFSnetworkcrawler is called for a specific network, which is required as a
# parameter
class BFSNetworkCrawler(NetworkCrawlerInterface):
  ""
  def __init__(self, network, seed_nodes=None, store_type = "gml", recover = True):
    """ Function to initialize a network crawler.
      Args:
        network: the network object to be crawled
        store_type: the type of storage. Valid values are "gml", "basic_shelve"
                    or "couchdb"
      Returns:
        An initialized crawler object
    """
    self.network = network
    self.seed_nodes = seed_nodes
    self.recover = recover
    store_class = GenericStore.get_store_class(store_type)
    # Buffer to store network data 
    self.gbuffer = GraphBuffer(self.network.label, store_class)
    #self.recover = recover
    # Priority queue in memory that keeps track of nodes to visit
    self.pqueue = PriorityQueue(store_class, store_name = self.network.label +
    "_state", recover=recover)

  def set_seed_nodes(self, seed_values):
    for seed in seed_values:
      self.add_to_crawl(seed)
  
  def add_to_crawl(self, node, priority=0):
    self.pqueue.push(node, priority)
    #self.visited[node] = False
  
  def __del__(self):
    self.close()
  
  def close(self):
    self.gbuffer.close()
    self.pqueue.close()

  def crawl(self, max_nodes = 10, checkpoint_frequency=100):
    """ Function to crawl the network using BFS.
        Works in two modes: fresh or recover. If recover is False, then
seed_nodes needs to be not None.
    """
    # if the crawl was stopped for some reason, recover parameter helps to
    # restart it from where it stopped.
    if self.recover and not self.pqueue.is_empty():
      # Just trying to minimize conflict. May lead to duplicate storage.
      node_counter = itertools.count(len(self.pqueue.visited.keys())+1)
      edge_counter = itertools.count(len(self.pqueue.visited.keys())+1)
      print("Node counter", node_counter)
    else:
      self.set_seed_nodes(self.seed_nodes)
      node_counter = itertools.count()
      edge_counter = itertools.count()
    iterations = 0
    while (not self.pqueue.is_empty()) and iterations < max_nodes:
      iterations += 1
      new_node = self.pqueue.pop()
      logging.info("Popped %s from queue and now starting to process it..." %new_node)
      print "Popped %s from queue and now starting to process it..." %new_node
      # Ignore if new_node has already been visited
      if new_node in self.pqueue.visited:
        continue
     
      
      # Get details about the current node 
      new_node_info = self.network.get_node_info(new_node)
      if new_node_info is None:
        error_msg = "Crawler: Error in fetching node info. Skipping node %s" %new_node
        logging.error(error_msg)
        print error_msg
        continue
        #raise NameError
      # Assume dict output for all stores.
      new_node_edges_info = self.network.get_edges_info(new_node)
      if new_node_edges_info is None:
        error_msg = "Crawler: Error in fetching node edges info. Skipping node %s" %new_node
        logging.error(error_msg)
        print error_msg
        continue
        #raise NameError
      
      logging.info("Crawler: Got information about %s" %new_node)
      
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
      self.gbuffer.store_node(new_node,  new_node_info)
      self.pqueue.mark_visited(new_node)
      for edge_info in new_node_edges_info:
        edge_info['id'] = next(edge_counter)
        self.gbuffer.store_edge(str(edge_info['id']), edge_info)
      logging.info("Processed %s \n" %new_node)
      print "Processed ", new_node
      if iterations % checkpoint_frequency == 0:
        self.pqueue.save_checkpoint()

    return
