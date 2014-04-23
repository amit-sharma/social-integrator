class NetworkDataPreparser:

  def __init__(self):
    self.nodes = {}
    self.items = {}
    self.edges = {}


  def read_nodes(self):
    raise NotImplementedError("NetworkDataPreparser: read_nodes is not implemented.")
