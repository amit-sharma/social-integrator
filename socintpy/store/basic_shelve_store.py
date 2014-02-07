import shelve
from socintpy.store.generic_store import GenericStore

class BasicShelveStore(GenericStore):
  def __init__(self, store_path, data_type="default"):
    self.data_shelve = shelve.open(store_path)
    self.data_type = data_type
  
  def __del__(self):
    self.data_shelve.close()

  def record(self, key, data_dict):
    #data_shelve = shelve.open(store_path)
    self.data_shelve[key] = data_dict
    #data_shelve.close()
  
  def fetch(self, pattern):
    #data_shelve = shelve.open(store_path)
    if pattern in self.data_shelve:
      data = self.data_shelve[pattern]
    else:
      data = None
    #data_shelve.close()
    return data
 
  def delete(self, key):
    del data_shelve[key]
  
  def get_all_data(self):
    data = []
    for key, value in self.data_shelve.iteritems():
      data.append((key, value))
    return data

  def close(self):
    self.data_shelve.close()
