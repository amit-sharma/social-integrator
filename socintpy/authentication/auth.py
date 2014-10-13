'''
It contains a base class for all authetication classes.
'''

class Authetication(object):
  def __init__(self):
    pass

  def add_auth(self, url, method, headers, parameters):
    """Add authentication headers."""
    raise NotImplementedError
