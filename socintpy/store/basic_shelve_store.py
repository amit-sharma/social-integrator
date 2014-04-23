import shelve,os
import bsddb, logging
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
    #print key.encode('ascii')
    #print data_dict
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
 
  def delete(self, pattern):
    key = pattern.encode('ascii')
    del self.data_shelve[key]
  
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

  def get_last_good_state_id(self, checkpoint_freq):
    last_id = -1
    try:
      state_ids = self.data_shelve.keys()
      if len(state_ids) > 0:
        last_id = max([int(v) for v in state_ids])
    except bsddb.db.DBPageNotFoundError:
      logging.error("Shelve corrupted. Trying recovery now.")
      print("Shelve corrupted.")
      prev_id = None
      curr_id = checkpoint_freq
      all_data = {}
      try:
        while True:
          if str(curr_id) in self.data_shelve:
            all_data[str(curr_id)] = self.data_shelve[str(curr_id)]
            prev_id = curr_id
            curr_id += checkpoint_freq
      except bsddb.db.DBPageNotFoundError:
        if prev_id is not None:
          last_id = prev_id
      self.data_shelve.close()
      self.data_shelve = shelve.open(self.store_path, flag='n')
      for key, val in all_data.iteritems():
        self.record(key, val)

    return last_id





  def get_dict(self):
    return self

  def close(self):
    self.data_shelve.close()

  def destroy_store(self):
    if os.path.isfile(self.store_path):
      os.remove(self.store_path)
