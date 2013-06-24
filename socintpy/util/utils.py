'''
some utility functions.
'''
import importlib

def convert_to_utf8_str(arg):
  # written by Michael Norton (http://docondev.blogspot.com/)
  if isinstance(arg, unicode):
    arg = arg.encode('utf-8')
  elif not isinstance(arg, str):
     arg = str(arg)
  return arg

def class_for_name(module_name, class_name):
  # Code from kocikowski's answer at http://stackoverflow.com/a/13808375
  # load the module, will raise ImportError if module cannot be loaded
  m = importlib.import_module(module_name)
  # get the class, will raise AttributeError if class cannot be
  # found
  c = getattr(m, class_name)
  return c
