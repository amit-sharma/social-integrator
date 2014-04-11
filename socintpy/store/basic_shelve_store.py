import shelve,os
from socintpy.store.generic_store import GenericStore
#from pprint import pprint

class BasicShelveStore(GenericStore):
  def __init__(self, store_path, data_type="default"):
    self.store_path = store_path
    self.data_shelve = shelve.open(store_path)
    self.data_type = data_type
  
  def __del__(self):
    self.data_shelve.close()

  def record(self, key, data_dict):
    #data_shelve = shelve.open(store_path)
    self.data_shelve[key.encode('ascii')] = data_dict
    #data_shelve.close()
  
  def fetch(self, pattern):
    #data_shelve = shelve.open(store_path)
    pattern = pattern.encode('ascii')
    if pattern in self.data_shelve:
      data = self.data_shelve[pattern]
    else:
      data = None
    #data_shelve.close()
    return data
 
  def delete(self, key):
    key = pattern.encode('ascii')
    del data_shelve[key]
  
  def ordered_values(self):
    new_arr = []
    for val in self.data_shelve.values():
      new_arr.append((val['created_time'], val))
    return sorted(new_arr, key=lambda x: x[0])
  
  def get_all_data(self):
    data = []
    for key, value in self.data_shelve.iteritems():
      data.append((key, value))
    return data

  def get_maximum_id(self):
    max_id = -1
    if self.data_shelve:
      max_id= max([int(val['id']) for val in self.data_shelve.values()])
    print max_id
    return max_id
  
  def get_dict(self):
    return self

  def close(self):
    self.data_shelve.close()

  def destroy_store(self):
    if os.path.isfile(self.store_path):
      os.remove(self.store_path)
