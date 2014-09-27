## Class to store a network in GML format

from socintpy.store.generic_store import GenericStore
import codecs


class GMLStore(GenericStore):
    def __init__(self, store_path, data_type):
        self.outfile = codecs.open(store_path, encoding="utf-8", mode="w")
        self.data_type = data_type

    def __del__(self):
        self.outfile.close()

    def record(self, data):
        itemdata = data.values()[0]
        data_str = "%s [\n" % self.data_type
        for key, value in itemdata.iteritems():
            data_str += "\t" + unicode(key) + "\t" + unicode(value) + "\n"
        data_str += "]\n"
        self.outfile.write(data_str)

    def fetch(self, pattern):
        pass

    def close(self):
        self.outfile.close()
