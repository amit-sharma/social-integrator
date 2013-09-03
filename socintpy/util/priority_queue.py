## An implementation of a priority queue.
## Provides push and pop and some custom application based functions

import itertools
import heapq

class PriorityQueue:
  def __init__(self, store_class = None, store_name = None):
    self.queue = []
    self.queue_dict = {}
    self.counter = itertools.count()
    if store_class:
      self.update_counter = itertools.count()
      self.state_store = store_class(store_name, data_type = "crawl_state")
  
  def __del__(self):
    self.state_store.close()

  def push(self, node, priority = 0):
    entry_id = next(self.counter)
    entry = [priority, entry_id, node]
    heapq.heappush(self.queue, entry)
    self.queue_dict[node] = entry
    if self.state_store:
      self.state_store.record(str(next(self.update_counter)), {'node': node,
        'action':'push', 'priority':priority}) 
    #return entry_id

  def mark_removed(self, node):
    entry = self.queue_dict.pop(node)
    entry_priority = entry[0]
    entry[2] = "REMOVD"
    if self.state_store:
      self.state_store.record(str(next(self.update_counter)), {'node': node,
        'action': 'mark_removed', 'priority': entry_priority})
    return entry_priority
  
  def pop(self):
    if not self.queue:
      raise KeyError("pop from empty queue.")
    found = False
    node = None
    entry_priority = None
    while self.queue and not found:
      entry = heapq.heappop(self.queue)
      entry_id = entry[1]
      entry_priority = entry[0]
      node = entry[2]
      if node != "REMOVD": 
        found = True
        del self.queue_dict[node]
    if self.state_store:
      self.state_store.record(str(next(self.update_counter)), {'node': node,
        'action': 'pop', 'priority': entry_priority})
    if found:
      return node
    else:
      raise KeyError("pop from empty queue.")
  
  def is_empty(self):
    return not self.queue_dict

  #TODO resolve the last pop, that should not be rerun
  def rerun_history(self):
    for key, value in self.state_store:
      if value['action'] == "push":
        self.push(value['node'], value['priority'])
      elif value['action']  == "mark_removed":
        self.mark_removed(value['node'])
      elif value['action'] == "pop":
        self.pop()
