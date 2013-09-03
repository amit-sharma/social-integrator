##The base class for api methods.

#TODO Where is this class taken from? Please cite.

#import sys, os
#sys.path.insert(1, os.path.join(sys.path[0], '..'))
from socintpy.util.utils import *
import httplib
import urllib
import re
import time
import logging

path_variable_template = re.compile('{\w+}')

class APICaller(object):
  
  def __init__(self):
    self.parameters = {}
    self.path = None
    self.final_path = None
    self.prefix = 'http://'

  def set_config(self, config):
    self.path = config['path']
    self.allowed_param = config.get('allowed_param', [])
    #self.method = config.get('method', 'GET')
    self.require_auth = config.get('require_auth', False)
    self.search_api = config.get('search_api', False)
    self.use_cache = config.get('use_cache', True)
    #self.api_key = config.get('api_key', None)


  def set_up(self, api, api_params):
    self.api = api
    #self.has_more_data = kargs.pop('has_more_data', None)
    self.method = api_params.pop('http_method', 'GET')
    self.post_data = api_params.pop('post_data', None)
    self.headers = api_params.pop('headers', {})
    self.build_parameters(api_params)
    self.build_path()
    self.headers['Host'] = self.api.host
    if api.secure:
      self.prefix = 'https://'
  
  def build_parameters(self, params_dict):
    self.parameters={}

    for k, arg in params_dict.items():
      if not arg:
        continue
      if k in self.parameters:
        raise NameError('Multiple values for parameter %s supplied!' % k)
      self.parameters[k] = convert_to_utf8_str(arg)

  def build_path(self):
    # replace parameters defined in the path
    self.final_path = self.path

  def execute(self):
    # Build the request URL
    url = self.api.api_root + self.final_path
    if self.parameters:
      url = '%s?%s' % (url, urllib.urlencode(self.parameters))

    # see whether results are in cache
    if self.use_cache and self.api.cache and self.method == 'GET':
      cache_result = self.api.cache.get(url)
      # if cache result found and not expired, return it
      if cache_result:
        return cache_result

    # Continue attempting request until successful
    # or maximum number of retries is reached.
    retry = 0
    while retry < self.api.retry_count + 1:
      if self.api.secure:
        conn = httplib.HTTPSConnection(self.api.host)
      else:
        conn = httplib.HTTPConnection(self.api.host)

      # Apply authentication
      if self.api.auth:
        self.api.auth.add_auth(self.prefix + self.host + url,
            self.method, self.headers, self.parameters)

      # Execute request
      try:
        logging.debug("Fetching %s from %s" %(url, self.api.host))
        #print url, self.api.host, self.method
        conn.request(self.method, url, headers=self.headers, body=self.post_data)
        resp = conn.getresponse()
      except Exception, e:
        raise NameError('Failed to send request: %s' % e)

      # Exit request loop if non-retry error code
      if self.api.retry_errors:
         if resp.status not in self.api.retry_errors: break
      else:
        if resp.status == 200: break

      # Sleep before retrying request again
      time.sleep(self.api.retry_delay)
      retry += 1

    # If an error was returned, throw an exception
    self.api.last_response = resp
    #print resp.read()
    if resp.status != 200:
      error_msg = "Error response: status code = %s" % resp.status
      raise NameError(error_msg)

    # Parse the response payload
    result = resp.read()
    conn.close()

    # Store result into cache if one is available.
    if self.use_cache and self.api.cache and self.method == 'GET' and result:
      self.api.cache[url] = result
    return result
    
