class NetworkDataPreparser:
    def __init__(self):
        self.nodes = {}
        self.items = {}
        self.edges = {}
        self.interaction_types = []


    def read_nodes(self):
        raise NotImplementedError("NetworkDataPreparser: read_nodes is not implemented.")
