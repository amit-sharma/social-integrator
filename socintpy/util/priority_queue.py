## An implementation of a priority queue.
## Provides push and pop and some custom application based functions

import itertools
import heapq
import logging
import time
from pprint import pprint

class PriorityQueue:
  def __init__(self, store_class = None, store_name = None):
    self.queue = []
    self.queue_dict = {}
    self.counter = itertools.count()
    if store_class is not None:
      self.update_counter = itertools.count()
      self.state_store = store_class(store_name, data_type = "crawl_state")
  
  def __del__(self):
    self.state_store.close()

  def push(self, node, priority = 0, rerun = False):
    entry_id = next(self.counter)
    entry = [priority, entry_id, node]
    heapq.heappush(self.queue, entry)
    self.queue_dict[node] = entry
    if self.state_store is not None and not rerun:
      self.state_store[str(next(self.update_counter))] = {'node': node,
        'action':'push', 'priority':priority, 'created_time':time.time()}
    #return entry_id

  def mark_removed(self, node, rerun = False):
    entry = self.queue_dict.pop(node)
    entry_priority = entry[0]
    entry[2] = "REMOVD"
    if self.state_store is not None and not rerun:
      self.state_store[str(next(self.update_counter))] = {'node': node,
        'action': 'mark_removed', 'priority': entry_priority, 'created_time':time.time()}
    return entry_priority
  
  def pop(self, rerun = False):
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
    if self.state_store is not None and not rerun:
      self.state_store[str(next(self.update_counter))] = {'node': node,
        'action': 'pop', 'priority': entry_priority, 'created_time': time.time()}
    if found:
      return node
    else:
      raise KeyError("pop from empty queue.")
  
  def is_empty(self):
    return not self.queue_dict

  #TODO resolve the last pop, that should not be rerun
  def rerun_history(self):
    rerun_action_max = None 
    action_counter = 0
    for created_time, doc in self.state_store.ordered_values():
      if doc['action'] == 'pop':
        rerun_action_max = action_counter
      action_counter += 1
    
    action_counter = 0  
    for created_time, doc in self.state_store.ordered_values():
      if action_counter >= rerun_action_max:
        break
      if doc['action'] == "push":
        self.push(doc['node'], doc['priority'], rerun=True)
      elif doc['action']  == "mark_removed":
        self.mark_removed(doc['node'], rerun = True)
      elif doc['action'] == "pop":
        self.pop(rerun = True)
      action_counter += 1
