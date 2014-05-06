class NetworkDataPreparser:
    def __init__(self):
        self.nodes = {}
        self.items = []
        self.edges = []
        self.interaction_types = []


    def read_nodes(self):
        raise NotImplementedError("NetworkDataPreparser: read_nodes is not implemented.")

    def get_nodes_iterable(self, should_have_friends= False, should_have_interactions=False):
        for k, v in self.nodes.iteritems():
            if should_have_friends and not v.has_friends:
                continue
            if should_have_interactions and not v.has_interactions:
                continue
            yield k, v

    def get_nonfriends_iterable(self, node):
        for k, v in self.nodes.iteritems():
            if k not in node.friends and k!=node.uid:
                yield k, v

