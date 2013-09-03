## An abstract class declaring the crawler interface. Each crawler must
## set_seed_nodes and implement a function for crawling.
class NetworkCrawlerInterface:
  def set_seed_nodes(nodes_list):
    raise NotImplementedError
  
  def crawl(max_iterations, max_nodes):
    raise NotImplementedError
