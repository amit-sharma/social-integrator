""" Retrieved from http://www.randomwalking.com/snippets/couchshelve.text

"""
#!/usr/bin/env python

# Copyright (C) Michael Ihde 2010 <mike.ihde@randomwalking.com>
#
# Distributed under the Python License

"""Manage a "shelf" of pickled objects with a couchdb backend.

This module provides a API identical to the standard python
shelve module, except instead of using a dbm database backend
it uses couchdb for a backend.

To summarize the interface (key is a string, data is an arbitrary
object):

        import couchshelve
        d = couchshelve.open('http://localhost:5984/example') # open the couch database 'example'

        d[key] = data   # store data at key (overwrites old data if
                        # using an existing key)
        data = d[key]   # retrieve a COPY of the data at key (raise
                        # KeyError if no such key)
        del d[key]      # delete data stored at key (raises KeyError
                        # if no such key)
        flag = d.has_key(key)   # true if the key exists; same as "key in d"
        list = d.keys() # a list of all existing keys
        list = d.values() # a list of all existing values (very slow!)
        list = d.items()  # a list of all existing key-value pairs (very slow!)
        iter = d.iteritems()  # an iterable that goes through all key-value pairs

        d.close()       # close it

Just like the standard shelve module, couchshelve won't persist mutable entries
automatically.  Again, just like the standard shelve module you can pass the
keyword argument writeback=True in the call to couchshelve.open.  All the
normal standard caveats apply when using writeback, so you should read the
shelve documenation closely.

One of the major differences between shelve and couchshelve is that couchshelve
is safe to use with multiple simultaneous readers and writers.  If you have
more than one writer, the default behavior is for the save operation to
overwrite whatever value is in the database.

However, if their are a large number concurrent writers, their is a possibility
that a couchdb conflict will occur.  The default behavior is to continue to try
the write until it succeeds.  Therefore, the ordering of writes is not
guaranteed nor is the timeliness of the writes guaranteed.  

As an alternative, you can pass the keyword argument raiseconflicts=True and
any attempt to write a key will raise a ConflictError and update the cache for
that object.  You will then need to determine the correct action you wish to
take.  This will unfortunately invalidate any references to the old cache
objects, so their changes will not be persisted.  Therefore, it is unwise to
specify both writeback and raiseconflicts unless you know what you are getting
into.
"""

import couchquery
import UserDict
import json

# Try using cPickle
try:
    import cPickle as pickle
except ImportError:
    import pickle

class ConflictError(StandardError):
    pass

class CouchShelf(UserDict.DictMixin):
    """A 'shelf' implemented using couchdb as a backend
    
    See the module's __doc__ string for an overview of the interface.
    """
    __MODIFYING_OPERATION = 0
    __NON_MODIFYING_OPERATION = 1

    # Shorthand versions
    __M_OP = __MODIFYING_OPERATION
    __NM_OP = __NON_MODIFYING_OPERATION

    def __init__(self, uri, flag='c', writeback=False, raiseconflicts=False):
        self._db = couchquery.Database(uri)
        if flag == 'n':
            couchquery.deletedb(self._db)
        if flag in ('n', 'c'):
            # the easy install version of couchdb doesn't
            # yet have the exists() function
            response = self._db.http.get('')
            if response.status == 404:
                couchquery.createdb(self._db)
        self._flag = flag
        self._writeback = writeback
        self._raiseconflicts = raiseconflicts
        self._cache = {}

    def __assertValidState(self, operationtype):
        if self._db == None:
            raise ValueError
        if self._flag == 'r' and action == self.__MODIFYING_OPERATION:
            raise ValueError

    def __getitem__(self, key):
        self.__assertValidState(self.__NM_OP)

        try:
            value = self._cache[key]
        except KeyError:
            try:
                doc = self._db.get(key)
            except couchquery.CouchDBDocumentDoesNotExist:
                raise KeyError
            # couchquery return unicode
            value = pickle.loads(str(doc.value))
            if self._writeback:
                self._cache[key] = value
        return value 

    def __setitem__(self, key, value):
        self.__assertValidState(self.__M_OP)

        if self._writeback:
            self._cache[key] = value

        val = pickle.dumps(value)
        while True:
            try:
                doc = self._db.get(key)
                doc.value = val
            except couchquery.CouchDBDocumentDoesNotExist:
                doc = {"_id": key, "value": val}

            try:
                self._db.save(doc)
                break
            except couchquery.CouchDBException:
                if self._writeback:
                    self._cache[key] = self[key]
                if self._raiseconflicts:
                    raise ConflictError

    def __delitem__(self, key):
        self.__assertValidState(self.__M_OP)

        try:
            doc = self._db.get(key)
            self._db.delete(doc)
        except couchquery.CouchDBDocumentDoesNotExist:
            pass
        try:
            del self._cache[key]
        except KeyError:
            pass

    def keys(self):
        self.__assertValidState(self.__NM_OP)

        response = self._db.http.get("_all_docs")
        obj = dict( (str(k),v) for k,v in json.loads(response.body).iteritems() )
        keys = []
        for row in obj["rows"]:
            keys.append(str(row["id"]))
        return tuple(keys)

    def values(self):
        self.__assertValidState(self.__NM_OP)

        if self._db == None:
            raise ValueError

        values = []
        for key in self.keys():
            values.append(self[key])
        return tuple(values)

    def items(self):
        self.__assertValidState(self.__NM_OP)

        if self._db == None:
            raise ValueError

        items = []
        for key in self.keys():
            items.append((key, self[key]))
        return tuple(items)

    def iteritems(self):
        self.__assertValidState(self.__NM_OP)

        if self._db == None:
            raise ValueError

        for key in self.keys():
            yield (key, self[key])

    def sync(self):
        if self._db == None:
            raise ValueError

        if self._writeback and self._cache:
            self._writeback = False
            for key, value in self._cache.iteritems():
                self[key] = value
            self._writeback = True
            self._cache = {}

    def close(self):
        self.sync()
        self._db = None

def open(uri, flag='c', writeback=False, raiseconflicts=False):
    """Open a persistent dictionary (backed by couchdb) for reading and writing.

    The uri parameter is the URI for the underlying couchdb database.  The flag
    parameter is:
    
        'c' : open existing database for reading and writing, creating it if it doesn't exist ('default')
        'w' : open existing database for reading and writing
        'r' : open existing database for reading
        'n' : always create a new empty database, open for reading and writing

    See the module's __doc__ string for an overview of the interface.
    """
    return CouchShelf(uri, flag, writeback, raiseconflicts)
