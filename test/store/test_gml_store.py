import unittest
from socintpy.store.gml_store import GMLStore

class TestGMLStore(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_record(self):
    data1 = {'id':2, 'name':'XYZ', 'age':30, 'about':'awesome1!'}
    data2 = {'id':3, 'name':'ZAC', 'age':32, 'about':'awesome2!'}
    #data3 = {'id':5, 'name':'ABA', 'age':35, 'about':'awesome3!'}
    data_store = GMLStore('node.dat', data_type="node")
    data_store.record(data1)
    data_store.record(data2)
    #data_store.record(data3)
    
    data_store.close()

    test_data_str = "node [\n\tabout\tawesome1!\n\tage\t30\n\tid\t2\n\tname\tXYZ\n]\n" +  "node [\n\tabout\tawesome2!\n\tage\t32\n\tid\t3\n\tname\tZAC\n]\n"
    datafile = open('node.dat')
    data_str = "".join(datafile.readlines())
    datafile.close()
    print data_str
    print test_data_str
    return self.assertEqual(test_data_str, data_str, 
                            "Problem with GMLStore record function.")

if __name__ == "__main__":
  unittest.main()
