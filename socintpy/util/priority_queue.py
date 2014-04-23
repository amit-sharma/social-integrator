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
    self.visited = {}
    self.store_class = store_class
    # Assigning the type of store
    if self.store_class is not None:
      # counter for number of actions on the pqueue
      self.state_store = store_class(store_name, data_type = "crawl_state")
      #print "Initializing state store", store_name
      #pprint(self.state_store)
      #self.update_counter = itertools.count(self.state_store.get_maximum_id()+1)

  def __del__(self):
    self.close()

  def close(self):
    self.state_store.close()
 
  def destroy_state(self):
    self.state_store.destroy_store()

  def initialize(self, seed_nodes, do_recovery=True, checkpoint_freq=None):
    num_nodes_stored = 0
    num_edges_stored = 0

    for seed in seed_nodes:
      self.push(seed, priority=0)
    if self.store_class is not None and do_recovery:
      last_checkpoint_id=self.state_store.get_last_good_state_id(checkpoint_freq)
      #print("Last checkpoint", last_checkpoint_id)
      if last_checkpoint_id != -1:
        logging.info("Starting recovery of queue.")
        recovered_state_store = self.state_store[str(last_checkpoint_id)]
        #print(recovered_state_store)
        self.queue = recovered_state_store['queue']
        self.queue_dict = recovered_state_store['queue_dict']
        self.visited = recovered_state_store['visited']
        self.counter  = recovered_state_store['counter']
        num_nodes_stored = recovered_state_store['num_nodes_stored']
        num_edges_stored = recovered_state_store['num_edges_stored']
        logging.info("Ended recovery of queue with checkpoint id: " + str(last_checkpoint_id))

    return(num_nodes_stored, num_edges_stored)

  def push(self, node, priority = 0):
    """ Function to add a node to the queue, update the queue_dictionary  and 
        record the action taken in a state_store.
    """
    entry_id = next(self.counter)
    entry = [priority, entry_id, node]
    heapq.heappush(self.queue, entry)
    self.queue_dict[node] = entry
      #self.state_store[str(next(self.update_counter))] = {'node': node,
      #  'action':'push', 'priority':priority, 'created_time':time.time()}
    #return entry_id
  
  def mark_removed(self, node):
    """ Function that deletes a node by marking it as "REMOVD".
        This is because deleting nodes in a list otherwise is time consuming.
        Also removes the node from the queue dictionary.

        Returns:
          entry_priority: the priority of the node now marked as removd
    """
    entry = self.queue_dict.pop(node)
    entry_priority = entry[0]
    entry[2] = "REMOVD"
      #self.state_store[str(next(self.update_counter))] = {'node': node,
      #  'action': 'mark_removed', 'priority': entry_priority, 'created_time':time.time()}
    return entry_priority

  def pop(self):
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
      #self.state_store[str(next(self.update_counter))] = {'node': node,
      #  'action': 'pop', 'priority': entry_priority, 'created_time': time.time()}
    if found:
      return node
    else:
      raise KeyError("pop from empty queue.")
  
  def is_empty(self):
    return not self.queue_dict
  
  def save_checkpoint(self, num_nodes_stored, num_edges_stored, store_freq):
    self.state_store[str(num_nodes_stored)] = {'id':num_nodes_stored, 'queue':self.queue, 'queue_dict':self.queue_dict,
                                           'num_edges_stored': num_edges_stored, 'num_nodes_stored': num_nodes_stored,
                                           'visited': self.visited, 'counter':self.counter}
    try:
      self.state_store.delete(str(num_nodes_stored-2*store_freq))
    except KeyError:
      logging.error("Could not delete from the state_store. Id was %d" %(num_nodes_stored -2*store_freq))

    return

  def mark_visited(self, node):
    self.visited[node] = True

  def print_size(self):
    print("Size of queue",len(self.queue), len(self.queue_dict), len(self.visited))
