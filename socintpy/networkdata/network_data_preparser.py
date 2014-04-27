class NetworkDataPreparser:
    def __init__(self):
        self.nodes = {}
        self.items = {}
        self.edges = {}
        self.interaction_types = []


    def read_nodes(self):
        raise NotImplementedError("NetworkDataPreparser: read_nodes is not implemented.")

    def get_core_nodes_iterable(self):
        for k, v in self.nodes.iteritems():
            if v.is_core:
                yield k, v

    def get_nonfriends_iterable(self, node):
        for k, v in self.nodes.iteritems():
            if k not in node.friends and k!=node.uid:
                yield k, v

