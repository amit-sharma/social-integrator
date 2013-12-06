## An implementation of a network crawler, using BFS

from socintpy.networkcrawl.crawler_interface import NetworkCrawlerInterface
from socintpy.util.priority_queue import PriorityQueue
from socintpy.store.graph_buffer import GraphBuffer
from socintpy.store.generic_store import GenericStore
import itertools

# BFSnetworkcrawler is called for a specific network, which is required as a
# parameter
class BFSNetworkCrawler(NetworkCrawlerInterface):
  def __init__(self, network, store_type = "gml", recover = False):
    self.network = network
    store_class = GenericStore.get_store_class(store_type)
    self.gbuffer = GraphBuffer(self.network.label, store_class)
    self.recover = recover
    self.pqueue = PriorityQueue(store_class, store_name = self.network.label +
    "_state")
    #self.visited = {}
    if self.recover:
      self.pqueue.rerun_history()

  def set_seed_nodes(self, seed_values):
    for seed in seed_values:
      self.add_to_crawl(seed)
  
  def add_to_crawl(self, node, priority=0):
      self.pqueue.push(node, priority)
      #self.visited[node] = False
      
  def crawl(self, seed_nodes, max_nodes = 10):
    if not self.recover:
      self.set_seed_nodes(seed_nodes)

    node_counter = itertools.count()
    edge_counter = itertools.count()
    iterations = 0 
    while (not self.pqueue.is_empty()) and iterations < max_nodes:
      new_node = self.pqueue.pop()
      if new_node in self.gbuffer.nodes_store:
        continue
      
      new_node_info = self.network.get_node_info(new_node)
      print "Processing", new_node
      # Assume dict output for all stores.
      new_node_edges_info = self.network.get_edges_info(new_node)
      for edge_info in new_node_edges_info:
        node = edge_info['target']
        if node not in self.gbuffer.nodes_store:
          if node in self.pqueue.queue_dict:
            node_priority = self.pqueue.mark_removed(node)
            updated_priority = node_priority - 1
            self.add_to_crawl(node, updated_priority)
          else:
            self.add_to_crawl(node)
      
      new_node_info['id'] = next(node_counter)
      self.gbuffer.store_node(new_node,  new_node_info)
      for edge_info in new_node_edges_info:
        edge_info['id'] = next(edge_counter)
        self.gbuffer.store_edge(str(edge_info['id']), edge_info)
      #self.visited[new_node] = True
      iterations += 1
      print "Processed", new_node, "\n"
    self.gbuffer.close()
    return
