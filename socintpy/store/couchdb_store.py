"""An implementation of GenericStore for couchdb backend

A class that exposes a dictionary-like interface for couchdb database
"""

import couchdb
from socintpy.store.generic_store import GenericStore
import logging


class CouchDBStore(GenericStore):
    def __init__(self, store_path, data_type="default"):
        self.couch_server = couchdb.Server()
        self.sanitized_couchname = store_path.replace(".", "").lower()
        try:
            self.couch = self.couch_server.create(self.sanitized_couchname)
        except couchdb.http.PreconditionFailed:
            self.couch = self.couch_server[self.sanitized_couchname]
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
            #raise Exception
            logging.error("The doc_id %s already exists in the couch" % doc_id)

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

    def ordered_values(self):
        map_fun = '''function(doc) {
                  emit(doc.created_time, doc)}'''
        for row in self.couch.query(map_fun):
            yield (row.key, row.value)

    def get_maximum_id(self):
        #return max(self.keys())
        max_id = -1
        if len(self.couch) > 0:
            map_fun = '''function(doc){
                  emit(doc.id, null)}'''
            rows = self.couch.query(map_fun)
            max_id = max([int(row.key) for row in rows])
        return max_id

    def get_dict(self):
        new_dict = {}
        for doc_id in self.couch:
            simple_dict = dict(self.couch[doc_id])
            data_key = simple_dict.pop('_id')
            simple_dict.pop('_rev')
            new_dict[data_key] = simple_dict

        return new_dict

    def close(self):
        #self.couch_connection.close()
        pass

    def destroy_store(self):
        self.couch_server.delete(self.sanitized_couchname)
