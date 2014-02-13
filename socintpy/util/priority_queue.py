## An implementation of a priority queue.
## Provides push and pop and some custom application based functions
##
## Features a dictionary that keeps track of the nodes in the queue. This is 
## helpful when searching for a node.

import itertools
import heapq
import logging
import time
from pprint import pprint

class PriorityQueue:
  def __init__(self, store_class = None, store_name = None):
    self.queue = []  # a list that has the data
    self.queue_dict = {} # a dictionary indexing the nodes
    self.counter = itertools.count() # number of nodes
    
    # Assigning the type of store
    if store_class is not None:
      # counter for number of actions on the pqueue
      self.state_store = store_class(store_name, data_type = "crawl_state")
      self.update_counter = itertools.count(self.state_store.get_maximum_id()+1)  

  def __del__(self):
    self.state_store.close()
  
  def destroy_state(self):
    self.state_store.destroy_store()

  def push(self, node, priority = 0, rerun = False):
    """ Function to add a node to the queue, update the queue_dictionary  and 
        record the action taken in a state_store.
    """
    entry_id = next(self.counter)
    entry = [priority, entry_id, node]
    heapq.heappush(self.queue, entry)
    self.queue_dict[node] = entry
    if self.state_store is not None and not rerun:
      self.update_state_store(node, 'push', priority)
      #self.state_store[str(next(self.update_counter))] = {'node': node,
      #  'action':'push', 'priority':priority, 'created_time':time.time()}
    #return entry_id
  
  def mark_removed(self, node, rerun = False):
    """ Function that deletes a node by marking it as "REMOVD".
        This is because deleting nodes in a list otherwise is time consuming.
        Also removes the node from the queue dictionary.

        Returns:
          entry_priority: the priority of the node now marked as removd
    """
    entry = self.queue_dict.pop(node)
    entry_priority = entry[0]
    entry[2] = "REMOVD"
    if self.state_store is not None and not rerun:
      self.update_state_store(node, 'mark_removed', entry_priority)
      #self.state_store[str(next(self.update_counter))] = {'node': node,
      #  'action': 'mark_removed', 'priority': entry_priority, 'created_time':time.time()}
    return entry_priority

  def pop(self, rerun = False):
    """ Function to remove an element from the queue based on its priority. 
        REMOVD elements are ignored.
    """
    if not self.queue:
      raise KeyError("pop from empty queue.")
    found = False
    node = None
    entry_priority = None
    # Loop through until find an element that is not "REMOVD"
    while self.queue and not found:
      entry = heapq.heappop(self.queue)
      entry_id = entry[1]
      entry_priority = entry[0]
      node = entry[2]
      if node != "REMOVD": 
        found = True
        del self.queue_dict[node]
    if self.state_store is not None and not rerun:
      self.update_state_store(node, 'pop', entry_priority)
      #self.state_store[str(next(self.update_counter))] = {'node': node,
      #  'action': 'pop', 'priority': entry_priority, 'created_time': time.time()}
    if found:
      return node
    else:
      raise KeyError("pop from empty queue.")
  
  def is_empty(self):
    return not self.queue_dict

  def rerun_history(self):
    """ Function to rerun the actions in order. This could have been simple,
        except that we would not like to run actions on and after the last pop.
        That node may not have been fully processed.
    """
    rerun_action_max = None 
    action_counter = 0
    # rerun_action_max is the number of pop actions done in history - 1
    for created_time, doc in self.state_store.ordered_values():
      if doc['action'] == 'pop':
        rerun_action_max = action_counter
      action_counter += 1
    
    # rerun until the last pop action
    pop_counter = 0  
    #pprint(self.state_store.ordered_values())
    for created_time, doc in self.state_store.ordered_values():
      if pop_counter >= rerun_action_max:
        break
      
      #print self.queue_dict
      if doc['action'] == "push":
        self.push(doc['node'], doc['priority'], rerun=True)
      elif doc['action']  == "mark_removed":
        self.mark_removed(doc['node'], rerun = True)
      elif doc['action'] == "pop":
        self.pop(rerun = True)
        pop_counter += 1

  def update_state_store(self, node, action, priority):
    curr_counter = str(next(self.update_counter))   
    store_key = action + curr_counter
    self.state_store[store_key] = {'node': node,
       'action': action, 'priority': priority, 
       'id': curr_counter, 'created_time': time.time()}


