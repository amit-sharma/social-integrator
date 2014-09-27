""" An intermediary class to make it easier to store graph objects.

Has a node store and an edge store as its attributes. Provides semantically meaningful
functions such as store_node and store_edge.
"""

class GraphBuffer:
    def __init__(self, label, StoreClass, output_dir=""):
        self.graph_label = label
        self.nodes_store = StoreClass(output_dir + label + "_nodes.db", data_type="node")
        self.edges_store = StoreClass(output_dir + label + "_edges.db", data_type="edge")
        return

    def store_node(self, key, node_info):
        self.nodes_store[key] = node_info

    def store_edge(self, key, edge_info):
        self.edges_store[key] = edge_info

    def store_edges(self, keys, edges_data):
        self.edges_store.record_many(keys, edges_data)

    def close(self):
        self.nodes_store.close()
        self.edges_store.close()

    def destroy_stores(self):
        self.nodes_store.destroy_store()
        self.edges_store.destroy_store()

