import unittest
from socintpy.util.priority_queue import PriorityQueue

class PriorityQueueTestCase(unittest.TestCase):
  def setUp(self):
    self.pqueue = PriorityQueue()
    self.test_data = [(10, "Phy"), (12, "Maths"), (30, "Chem"), (25, "Bio"),
                      (41, "English")]
  def tearDown(self):
    pass

  def test_push(self):
    for data_element in self.test_data:
      self.pqueue.push(data_element)
    test_list=[]
    test_dict = {}
    for i in range(len(self.test_data)):
      entry = [0, i, self.test_data[i]]
      test_list.append(entry)
      test_dict[self.test_data[i]] = entry
    self.assertListEqual(self.pqueue.queue, test_list, "Problem with push to queue.")  
    self.assertDictEqual(self.pqueue.queue_dict, test_dict,
                         "Problem with push to queue.")
  def test_remove(self):
    for data_element in self.test_data[:3]:
      self.pqueue.push(data_element)
    node1 = self.test_data[1]
    self.pqueue.mark_removed(node1)

    self.pqueue.push(self.test_data[3])
    
    node3 = self.test_data[0]
    self.pqueue.mark_removed(node3)

    test_list=[]
    test_dict = {}
    for i in range(len(self.test_data[:4])):
      entry = [0, i, self.test_data[i]]
      test_list.append(entry)
      test_dict[self.test_data[i]] = entry
    test_list[0][2]="REMOVD"
    test_list[1][2]="REMOVD"
    del test_dict[self.test_data[0]]
    del test_dict[self.test_data[1]]
    #print test_list, test_dict
    #print self.pqueue.queue, self.pqueue.queue_dict
    self.assertListEqual(self.pqueue.queue, test_list, 
                         "Problem with mark_removed on PriorityQueue.")
    self.assertDictEqual(self.pqueue.queue_dict, test_dict, 
                         "Problem with mark_removed on PriorityQueue.")
      
  def test_pop(self):
    for data_element in self.test_data[:3]:
      self.pqueue.push(data_element)
    
    node1 = self.test_data[1]
    self.pqueue.mark_removed(node1)
    
    self.pqueue.pop()

    node1 = self.test_data[3]
    self.pqueue.push(node1)

    self.pqueue.pop()
    self.pqueue.pop()

    test_list = []
    test_dict = {}
    
    self.assertListEqual(self.pqueue.queue, test_list, 
                         "Problem with pop on PriorityQueue.")
    self.assertDictEqual(self.pqueue.queue_dict, test_dict, 
                         "Problem with pop on PriorityQueue.")

if __name__ == "__main__":
  unittest.main()
