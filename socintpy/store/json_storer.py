'''
A implementation of class Store for json files.
'''

import json

def JsonStore(object):
  def __init__(self, fp=None):
    self.output = fp

  def dump(self, result):
    json.dump(result, self.output)

