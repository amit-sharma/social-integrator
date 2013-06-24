class BaseCaller:
  def __init__(self, label, auth_handler, host, api_root, cache, secure,
               retry_count, retry_delay, retry_errors):
    self.label = label
    self.auth = auth_handler                                                     
    self.host = host                                                             
    self.api_root = api_root                                                     
    self.cache = cache                                                           
    self.secure = secure                                                         
    self.retry_count = retry_count                                               
    self.retry_delay = retry_delay                                               
    self.retry_errors = retry_errors 

	def get_node_info():
		raise NotImplementedError

	def get_connections():
		raise NotImplementedError
  
  def get_edges_info():
    raise NotImplementedError

	def get_followers():
		raise NotImplementedError
	
	def get_followees():
		raise NotImplementedError

	def get_favorites():
		raise NotImplementedError  
