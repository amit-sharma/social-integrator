import os,os.path
from sqlite3 import dbapi2 as sqlite
import sqlite3
import cPickle
from socintpy.store.generic_store import GenericStore

class SqliteStore(GenericStore):
  ''' dbdict, a dictionnary-like object for large datasets (several Tera-bytes) '''

  def __init__(self,dictName, data_type="default"):
    self.db_filename = "%s.sqlite" % dictName
    self.data_type = data_type
    if not os.path.isfile(self.db_filename):
      self.con = sqlite.connect(self.db_filename)
      self.con.execute("create table data (key PRIMARY KEY,value)")
    else:
      self.con = sqlite.connect(self.db_filename)

  def fetch(self, key):
    row = self.con.execute("select value from data where key=?",(key,)).fetchone()
    if not row: raise KeyError
    return cPickle.loads(str(row[0]))

  def record(self, key, item):
    pdata = cPickle.dumps(item, cPickle.HIGHEST_PROTOCOL)
    item = sqlite3.Binary(pdata)
    if self.con.execute("select key from data where key=?",(key,)).fetchone():
      self.con.execute("update data set value=? where key=?",(item,key))
    else:
      self.con.execute("insert into data (key,value) values (?,?)",(key, item))
    self.con.commit()

  def delete(self, key):
    if self.con.execute("select key from data where key=?",(key,)).fetchone():
      self.con.execute("delete from data where key=?",(key,))
      self.con.commit()
    else:
       raise KeyError

  def iteritems(self):
    for row in self.con.execute("select key,value from data").fetchall():
      yield ( row[0], cPickle.loads(str(row[1])) )

  def keys(self):
    return [row[0] for row in self.con.execute("select key from data").fetchall()]

  def values(self):
    for row in self.con.execute("select value from data").fetchall():
      yield cPickle.loads(str(row[0]))

  def get_count_records(self):
    row = self.con.execute("select count(key) from data").fetchone()

    if row[0] is None:
      return -1
    else:
      return int(row[0])


  def get_maximum_idd(self):
    row = self.con.execute("select max(key) from data").fetchone()

    if row[0] is None:
      return -1
    else:
      return int(row[0])

  def get_last_good_state_id(self, checkpoint_freq):
    return self.get_maximum_idd()

  def close(self):
    self.con.close()

  def destroy_store(self):
    self.con.execute("drop table if exists data")
    self.con.commit()
