class NetworkDataPreparser():
    def __init__(self):
        self.nodes = []
        self.items = []
        self.edges = []
        self.interaction_types = []


    def read_nodes(self):
        raise NotImplementedError("NetworkDataPreparser: read_nodes is not implemented.")

    def get_nodes_iterable(self, should_have_friends= False, should_have_interactions=False):
        for v in self.nodes:
            if should_have_friends and not v.has_friends:
                continue
            if should_have_interactions and not v.has_interactions:
                continue
            yield v

    def get_nonfriends_iterable(self, node):
        for v in self.nodes:
            if v.uid not in [f[0] for f in node.friends] and v.uid != node.uid:
                yield v

