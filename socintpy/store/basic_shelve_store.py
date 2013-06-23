import shelve
from socintpy.store.generic_store import GenericStore

class BasicShelveStore(GenericStore):
  def __init__(self):
    pass

  def record(self, data_dict, store_path):
    data_shelve = shelve.open(store_path)
    data_shelve.update(data_dict)
    data_shelve.close()
  
  def fetch(self, pattern, store_path):
    data_shelve = shelve.open(store_path)
    data = data_shelve[pattern]
    data_shelve.close()
    return data
