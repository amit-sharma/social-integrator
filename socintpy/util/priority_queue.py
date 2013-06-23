import itertools
import heapq

class PriorityQueue:
  def __init__(self):
    self.queue = []
    self.queue_dict = {}
    self.counter = itertools.count()

  def push(self, node, priority = 0):
    entry_id = next(self.counter)
    entry = [priority, entry_id, node]
    heapq.heappush(self.queue, entry)
    self.queue_dict[node] = entry

  def mark_removed(self, node):
    entry = self.queue_dict.pop(node)
    entry_priority = entry[0]
    entry[2] = "REMOVD"
    return entry_priority
  
  def pop(self):
    if not self.queue:
      raise KeyError("pop from empty queue.")
    found = False
    node = None
    while self.queue and not found:
      entry = heapq.heappop(self.queue)
      entry_id = entry[1]
      node = entry[2]
      if node != "REMOVD": 
        found = True
        del self.queue_dict[node]
    if found:
      return node
    else:
      raise KeyError("pop from empty queue.")
  
  def is_empty(self):
    return not self.queue_dict
