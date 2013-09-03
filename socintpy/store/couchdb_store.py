import couchdb
from socintpy.store.generic_store import GenericStore
import logging

class CouchDBStore(GenericStore):
  def __init__(self, store_path, data_type="default"):
    self.couch_server = couchdb.Server()
    sanitized_couchname = store_path.replace(".","")
    try:
      self.couch = self.couch_server.create(sanitized_couchname)
    except couchdb.http.PreconditionFailed:
      self.couch = self.couch_server[sanitized_couchname]
    self.data_type = data_type
  
  def __del__(self):
    #self.couch_connection.close()
    pass

  def record(self, doc_id, doc_data):
    #doc_data = data_dict.values()[0]
    doc_data['_id'] = doc_id
    try:
      couch_doc_id, doc_rev = self.couch.save(doc_data)
    except couchdb.http.ResourceConflict:
      logging.error("The doc_id %s already exists in the couch"
          %doc_id)
  
  def fetch(self, doc_id):
    data = None
    if doc_id in self.couch:
      data = self.couch[doc_id]
    return data
  
  def get_all_data(self):
    docs_list = []
    for doc_id in self.couch:
      print doc_id
      docs_list.append((doc_id, self.couch[doc_id]))
    return docs_list

  def close(self):
    #self.couch_connection.close()
    pass
