from socintpy.networkcrawl.crawler_interface import NetworkCrawlerInterface
from socintpy.util.priority_queue import PriorityQueue

class BFSNetworkCrawler(NetworkCrawlerInterface):
  def __init__(self, network, store, node_info_storepath,
               node_connections_storepath):
    self.pqueue = PriorityQueue()
    self.store = store
    self.network = network
    self.node_info_storepath = node_info_storepath
    self.node_connections_storepath = node_connections_storepath
    self.visited = {}

  def set_seed_nodes(self, seed_values):
    for seed in seed_values:
      self.add_to_crawl(seed)
  
  def add_to_crawl(self, node, priority=0):
      self.pqueue.push(node, priority)
      #self.visited[node] = False
      
  def crawl(self):
    while not self.pqueue.is_empty():
      new_node = self.pqueue.pop()
      if new_node in self.visited:
        continue
      new_node_info = self.network.get_node_info(new_node)
      # Assume dict output for all stores.
      self.store.record(new_node_info, self.node_info_storepath)
      new_node_connections = self.network.get_connections(new_node)
      self.store.record({new_node: new_node_connections}, 
                        self.node_connections_storepath)
      # TODO also store these in a db
      for node in new_node_connections:
        if node not in self.visited:
          if node in self.pqueue.queue_dict:
            node_priority = self.pqueue.mark_removed(node)
            updated_priority = node_priority - 1
            self.add_to_crawl(node, updated_priority)
          else:
            self.add_to_crawl(node)
      self.visited[new_node] = True
