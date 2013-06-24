from socintpy.networkcrawl.crawler_interface import NetworkCrawlerInterface
from socintpy.util.priority_queue import PriorityQueue
from socintpy.store.graph_buffer import GraphBuffer
import itertools

class BFSNetworkCrawler(NetworkCrawlerInterface):
  def __init__(self, network, store_type = "gml"):
    self.pqueue = PriorityQueue()
    self.network = network
    self.gbuffer = GraphBuffer(self.network.label, store_type)
    self.visited = {}

  def set_seed_nodes(self, seed_values):
    for seed in seed_values:
      self.add_to_crawl(seed)
  
  def add_to_crawl(self, node, priority=0):
      self.pqueue.push(node, priority)
      #self.visited[node] = False
      
  def crawl(self):
    node_counter = itertools.count()
    edge_counter = itertools.count()
    while not self.pqueue.is_empty():
      new_node = self.pqueue.pop()
      if new_node in self.visited:
        continue
      new_node_info = self.network.get_node_info(new_node)
      # Assume dict output for all stores.
      new_node_info['id'] = next(node_counter)
      self.gbuffer.store_node({str(new_node_info['id']): new_node_info})
      new_node_edges_info = self.network.get_edges_info(new_node)
      for edge_info in new_node_edges_info:
        edge_info['id'] = next(edge_counter)
        self.gbuffer.store_edge({str(edge_info['id']): edge_info})
      
      for edge_info in new_node_edges_info:
        node = edge_info['target']
        if node not in self.visited:
          if node in self.pqueue.queue_dict:
            node_priority = self.pqueue.mark_removed(node)
            updated_priority = node_priority - 1
            self.add_to_crawl(node, updated_priority)
          else:
            self.add_to_crawl(node)
      self.visited[new_node] = True
    self.gbuffer.close()
    return
