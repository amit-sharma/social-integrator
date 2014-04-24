import unittest
from socintpy.store.basic_shelve_store import BasicShelveStore
from socintpy.store.couchdb_store import CouchDBStore
from pprint import pprint
import os


class TestStore(unittest.TestCase):
    def setUp(self):
        self.filename = StoreClass.__name__ + 'teststore.db'
        self.data_type = "test"
        self.testdata = {}
        self.testdata['k1'] = {'id': 2, 'created_time': 1.2, 'name': 'XYZ', 'age': 30, 'about': 'awesome1!'}
        self.testdata['k2'] = {'id': 3, 'created_time': 1.1, 'name': 'ZAC', 'age': 32, 'about': 'awesome2!'}
        self.test_maxid = 3
        self.test_ordered_keys = ['k2', 'k1']
        self.data_store = self.initialize_data()

    def initialize_data(self):
        data_store = StoreClass(self.filename, self.data_type)
        for key, val in self.testdata.iteritems():
            data_store[key] = val
        return data_store

    def tearDown(self):
        self.data_store.close()
        self.data_store.destroy_store()

    def test_fetch(self):
        #data3 = {'id':5, 'name':'ABA', 'age':35, 'about':'awesome3!'}
        self.data_store.close()
        #test_data_str = "node [\n\tabout\tawesome1!\n\tage\t30\n\tid\t2\n\tname\tXYZ\n]\n" +  "node [\n\tabout\tawesome2!\n\tage\t32\n\tid\t3\n\tname\tZAC\n]\n"
        new_data_store = StoreClass(self.filename, self.data_type)

        pprint(dict(new_data_store))
        pprint(self.testdata)
        res = self.assertDictEqual(self.testdata, dict(new_data_store),
                                   "Problem with Store's record function.")
        new_data_store.close()
        return res

    def test_ordered_values(self):
        ordered_values = list(self.data_store.ordered_values())
        pprint(ordered_values)
        test_values = [(self.testdata[key]['created_time'], self.testdata[key]) for key in self.test_ordered_keys]
        return self.assertSequenceEqual(test_values, ordered_values, "Problem with Store's ordering function.")

    def test_max_id(self):
        maxid = self.data_store.get_maximum_id()
        # test_maxid = max([val['id'] for val in self.testdata.values()])
        return self.assertEqual(self.test_maxid, maxid, "Problem with Store's max id function.")


if __name__ == "__main__":
    store_class_list = [BasicShelveStore, CouchDBStore]
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStore)
    for store_class in store_class_list:
        StoreClass = store_class
        unittest.TextTestRunner().run(suite)
        print "DONE ", store_class
