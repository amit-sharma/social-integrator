""" File to test bfs network crawl.
"""
import unittest
from socintpy.caller.base_caller import BaseCaller
from socintpy.networkcrawl.bfs_networkcrawler import BFSNetworkCrawler
import shelve, os
from pprint import pprint


class TestWebCaller(BaseCaller):
  """ A dummy web caller
  """
  def __init__(self, label, edges):
    self.label = label
    self.edges = edges
    self.visit_order = []

  def get_node_info(self, node_id):
    self.visit_order.append(node_id)
    return {'id': node_id, 'name': "Hello, I am node %s" % node_id}
  
  def get_connections(self, node_id):
    return self.edges[node_id]
 
  def get_edges_info(self, node_id):
    edges_info = []
    for connection in self.get_connections(node_id):
      edges_info.append({'source': node_id, 'target': connection})
    return edges_info

  def get_visit_order(self):
      return self.visit_order

class TestBFSNetworkCrawler(unittest.TestCase):
  def setUp(self):
    self.network_edges = {"11":["1", "5", "8"], "1": ["11", "8"], 
                          "8": ["1", "11", "5"], "5": ["11", "8"]}
    self.node_info_data = None
    self.node_connections_data = None
    self.correct_visit_order = ["11","5","8","1"]
    self.crawler = None

  def tearDown(self):
    self.crawler.gbuffer.destroy_stores()
    self.crawler.pqueue.destroy_state()

  
  def test_crawl(self):
    # Running the bfs crawler
    num_nodes_before_crash=2
    webnetwork = TestWebCaller("test", self.network_edges)
    self.crawler = BFSNetworkCrawler(webnetwork, store_type=STORE_TYPE)
    self.crawler.crawl(seed_nodes=["11","5"], max_nodes=num_nodes_before_crash)
    
    node_visit_order = webnetwork.get_visit_order()
    self.compare_values(node_visit_order,
test_visit_order=self.correct_visit_order[:num_nodes_before_crash])   
    self.crawler.close() 

    self.crawler = BFSNetworkCrawler(webnetwork, store_type=STORE_TYPE)
    self.crawler.crawl(recover=True)

    node_visit_order = webnetwork.get_visit_order()
    print "Node visit order is", node_visit_order
    self.compare_values(node_visit_order,
test_visit_order=self.correct_visit_order)
      
    self.crawler.close()  
    
  def compare_values(self, real_visit_order, test_visit_order):     
    # Accessing the results of calling the source code
    self.node_info_data = self.crawler.gbuffer.nodes_store.get_dict()
    print self.node_info_data
    self.node_connections_data = self.crawler.gbuffer.edges_store.get_dict()
    self.node_visit_order = real_visit_order
    
    # Testing the results with expected values
    test_node_info = {}
    test_node_edges_data = {}
    #test_visit_order = ["11", "5", "8", "1"]
    node_counter = 0
    edge_counter = 0
    test_webnetwork =  TestWebCaller("test", self.network_edges)
    for i in test_visit_order:
      curr_node_info = test_webnetwork.get_node_info(i)
      curr_node_info['id'] = node_counter
      test_node_info.update({str(i): curr_node_info})
      curr_edges_info = test_webnetwork.get_edges_info(i)
      for edge_info in curr_edges_info:
        edge_info['id'] = edge_counter
        test_node_edges_data[str(edge_counter)] = edge_info
        edge_counter += 1
      node_counter += 1
    
   
    print self.node_visit_order
    pprint(self.node_info_data)
    pprint(test_node_info)
    #pprint(self.node_connections_data)
    self.assertListEqual(self.node_visit_order, test_visit_order,
        "Problem in order of BFS crawl")
    self.assertDictEqual(test_node_info, dict(self.node_info_data), 
                         "Nodes do not match: Problem in BFS crawl.")
    self.assertDictEqual(test_node_edges_data, dict(self.node_connections_data),
                         "Edges do not match: Problem in BFS crawl.")


if __name__ == "__main__":
  #STORE_TYPE = "basic_shelve"
  STORE_TYPE = "couchdb"
  unittest.main()


