'''
A base class for different stores.
'''

class GenericStore(object):
  def __init__(self):
    pass

  def record(data_dict, store_path):
    raise NotImplementedError

  def fetch(pattern, store_path):
    raise NotImplementedError
  
  def close(self):
    raise NotImplementedError
