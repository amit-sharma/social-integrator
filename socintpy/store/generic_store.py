##A base class for different stores.
from socintpy.util.utils import class_for_name
import UserDict

#TODO implement cache
class GenericStore(UserDict.DictMixin):
  def __init__(self):
    pass

  def __getitem__(self, key):
    return self.fetch(key)
  
  def __setitem__(self, key, value):
    self.record(key, value)

  def __delitem__(self, key):
    self.delete(key)
  
  def __contains__(self, key):
    data = self.fetch(key)
    if data is None:
      return False
    else:
      return True

  def keys(self):
    data = self.get_all_data()
    keys_list = []
    for key, value in data:
      keys_list.append(key)
    return tuple(keys_list)
  
  def values(self):
    data = self.get_all_data()
    values_list = []
    for key, value in data:
      values_list.append(value)
    return tuple(values_list)
  
  def items(self):
    return tuple(self.get_all_data())

  def iteritems(self):
    for key, value in self.get_all_data():
      yield (key, value)
  
  # Add the dictionary data_dict to the store, indexed by "key"
  def record(self, key, data_dict):
    raise NotImplementedError

  def fetch(self, pattern):
    raise NotImplementedError
  
  def delete(self, key):
    raise NotImplementedError
  
  def ordered_values(self):
    raise NotImplementedError
  def close(self):
    raise NotImplementedError
  
  def destroy_store(self):
    raise NotImplementedError

  @staticmethod
  def get_store_class(store_type):
    StoreClass = None
    if store_type == "basic_shelve":                                             
      StoreClass = class_for_name("socintpy.store.basic_shelve_store",           
                     "BasicShelveStore")                            
    elif store_type == "gml":                                                    
      StoreClass = class_for_name("socintpy.store.gml_store", "GMLStore")        
    elif store_type == "couchdb":                                                
      StoreClass = class_for_name("socintpy.store.couchdb_store", "CouchDBStore")
    return StoreClass
                   
