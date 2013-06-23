import shelve
from socintpy.store.generic_store import GenericStore

class BasicShelveStore(GenericStore):
  def __init__(self, store_path):
    self.data_shelve = shelve.open(store_path)
  
  def __del__(self):
    self.data_shelve.close()

  def record(self, data_dict):
    #data_shelve = shelve.open(store_path)
    self.data_shelve.update(data_dict)
    #data_shelve.close()
  
  def fetch(self, pattern, store_path):
    #data_shelve = shelve.open(store_path)
    data = self.data_shelve[pattern]
    #data_shelve.close()
    return data
  
  def close(self):
    self.data_shelve.close()
