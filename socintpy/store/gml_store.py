from socintpy.store.generic_store import GenericStore

class GMLStore(GenericStore):
  def __init__(self, store_path, data_type):
    self.outfile = open(store_path, "w")
    self.data_type = data_type
  def __del__(self):
    self.outfile.close()

  def record(self, data):
    data_str = "%s [\n" % self.data_type
    for key, value in data.iteritems():
      data_str += "\t" + str(key) + "\t" + str(value) + "\n" 
    data_str += "]\n"
    self.outfile.write(data_str)

  def fetch(self, pattern):
    pass

  def close(self):
    self.outfile.close()
