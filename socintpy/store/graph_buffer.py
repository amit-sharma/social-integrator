from socintpy.store.generic_store import GenericStore

class GraphBuffer:
  def __init__(self, label, StoreClass):
    self.graph_label = label
    self.nodes_store = StoreClass(label + "_nodes.db", data_type = "node")
    self.edges_store = StoreClass(label + "_edges.db", data_type = "edge")
    return

  def store_node(self, key, node_info):
    self.nodes_store[key] = node_info
  
  def store_edge(self, key, edge_info):
    self.edges_store[key] = edge_info

  def close(self):
    self.nodes_store.close()
    self.edges_store.close()


