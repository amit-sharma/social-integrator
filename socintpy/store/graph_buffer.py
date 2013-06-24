from socintpy.util.utils import class_for_name

class GraphBuffer:
  def __init__(self, label, store_type = "gml"):
    self.graph_label = label
    StoreClass = None
    if store_type == "basic_shelve":
      StoreClass = class_for_name("socintpy.store.basic_shelve_store",
                                  "BasicShelveStore")
    elif store_type == "gml":
      StoreClass = class_for_name("socintpy.store.gml_store", "GMLStore")
    self.nodes_store = StoreClass(label + "_nodes.db", data_type = "node")
    self.edges_store = StoreClass(label + "_edges.db", data_type = "edge")
    return

  def store_node(self, node_info):
    self.nodes_store.record(node_info)
  
  def store_edge(self, edge_info):
    self.edges_store.record(edge_info)

  def close(self):
    self.nodes_store.close()
    self.edges_store.close()


