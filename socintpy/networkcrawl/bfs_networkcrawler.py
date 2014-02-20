## An implementation of a network crawler, using BFS

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
  def __init__(self, network, store_type = "gml"):
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
    self.pqueue = PriorityQueue(store_class, store_name = self.network.label +
    "_state")

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

  #TODO recover parameter is redundant with seed_nodes. remove in a later version.  
  def crawl(self, seed_nodes = None, max_nodes = 10, recover = False):
    """ Function to crawl the network using BFS.
        Works in two modes: fresh or recover. If recover is False, then
seed_nodes needs to be not None.
    """
    # if the crawl was stopped for some reason, recover parameter helps to
    # restart it from where it stopped.
    if recover:
      logging.info("Starting recovery of queue.")
      self.pqueue.rerun_history()
      logging.info("Ended recovery of queue.")
      node_counter = itertools.count(self.gbuffer.nodes_store.get_maximum_id()+1)
      edge_counter = itertools.count(self.gbuffer.edges_store.get_maximum_id()+1)
    else:
      self.set_seed_nodes(seed_nodes)
      node_counter = itertools.count()
      edge_counter = itertools.count()
    iterations = 0 
    while (not self.pqueue.is_empty()) and iterations < max_nodes:
      iterations += 1
      new_node = self.pqueue.pop()
      # Ignore if new_node has already been visited
      if new_node in self.gbuffer.nodes_store:
        continue
     
      # Get details about the current node 
      new_node_info = self.network.get_node_info(new_node)
      if new_node_info is None:
        logging.error("Crawler: Error in fetching node info")
        raise NameError
      else:  
        logging.info("Crawler: Got information about %s" %new_node)
      # Assume dict output for all stores.
      new_node_edges_info = self.network.get_edges_info(new_node)
      print "Got all data"
      print "\n"
      if new_node_edges_info is None:
        logging.error("Crawler: Error in fetching node's edges info")
        raise NameError
      else:
        logging.info("Crawler: Got information about %s's edges" %new_node)
      
      for edge_info in new_node_edges_info:
        node = edge_info['target']
  
        #self.pqueue.state_store = None
        if node not in self.gbuffer.nodes_store:
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
      for edge_info in new_node_edges_info:
        edge_info['id'] = next(edge_counter)
        self.gbuffer.store_edge(str(edge_info['id']), edge_info)
      logging.info("Processed %s \n" %new_node)
      print "Processed ", new_node
    return
