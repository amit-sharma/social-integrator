import unittest
from socintpy.caller.caller_interface import CallerInterface
from socintpy.networkcrawl.bfs_networkcrawler import BFSNetworkCrawler
from socintpy.store.basic_shelve_store import BasicShelveStore
import shelve

class TestWebCaller(CallerInterface):
  def __init__(self, edges):
    self.edges = edges
    self.visit_order = []

  def get_node_info(self, node_id):
    self.visit_order.append(node_id)
    return {node_id: "Hello, I am node %s" % node_id} 
  
  def get_connections(self, node_id):
    return self.edges[node_id]
  
  def get_visit_order(self):
      return self.visit_order

class TestBFSNetworkCrawler(unittest.TestCase):
  def setUp(self):
    self.network_edges = {"11":["1", "5", "8"], "1": ["11", "8"], 
                          "8": ["1", "11", "5"], "5": ["11", "8"]}
    self.node_info_data = None
    self.node_connections_data = None

  def tearDown(self):
    #self.node_info_data.close()
    #self.node_connections_data.close()
    pass

  def test_crawl(self):
    webnetwork = TestWebCaller(self.network_edges)
    nodes_store = BasicShelveStore("node_info.dat")
    edges_store = BasicShelveStore("edges_info.dat")
    crawler = BFSNetworkCrawler(webnetwork, nodes_store, edges_store)
    crawler.set_seed_nodes(["11", "5"])
    crawler.crawl()

    self.node_info_data = shelve.open("node_info.dat")
    self.node_connections_data = shelve.open("edges_info.dat")
    self.node_visit_order = list(webnetwork.get_visit_order())

    test_node_info = {}
    test_node_connections = {}
    test_visit_order = ["11", "5", "8", "1"]
    for i in test_visit_order:
      test_node_info.update(webnetwork.get_node_info(i))
      test_node_connections[i] = webnetwork.get_connections(i)
    #print self.node_visit_order, self.node_info_data
    self.assertListEqual(self.node_visit_order, test_visit_order,
        "Problem in order of BFS crawl")
    self.assertDictEqual(test_node_info, dict(self.node_info_data), "Problem in BFS crawl.")
    self.assertDictEqual(test_node_connections, dict(self.node_connections_data),
      "Problem in BFS crawl")
    self.node_connections_data.close()
    self.node_info_data.close()

if __name__ == "__main__":
  unittest.main()


