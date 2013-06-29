class NetworkCrawlerInterface:
  def set_seed_nodes(nodes_list):
    raise NotImplementedError
  
  def crawl(max_iterations, max_nodes):
    raise NotImplementedError
