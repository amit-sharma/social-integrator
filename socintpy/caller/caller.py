'''
A class to implement general API call function.
'''
import time
import logging
from socintpy.caller.api_call_error import APICallError 

def call(caller, **config):
  '''Similar to the method in tweepy, but api_method can change
  so that we have different api methods for different websites'''

  # use config to init api caller
  caller.set_config(config)

  def _call(api, *args, **kargs):
    api_method = kargs['method']
    max_results = kargs.pop('max_results', None)
    response= ''
    total_results_fetched = 0
    fetch_more_data = True
    while fetch_more_data:
      api_params = api.get_default_params(api_method)
      api_params.update(kargs)
      #print api_params, custom_params, api.get_default_params()
      caller.set_up(api, api_params)
      timediff = time.time() - api.previous_call_time
      #print timediff, api.api_delay
      if api.api_delay and timediff < api.api_delay:
        time.sleep(api.api_delay - timediff)
      logging.debug("Time between calls = %f seconds" % (time.time() -
        api.previous_call_time))
      api.set_last_call_time(time.time())
      try:
        current_resp = caller.execute()
      except APICallError, e:
        raise APICallError(e)
      response += current_resp
      #print current_resp
      next_page_params, num_results = api.__class__.analyze_page(current_resp, api_method)
      if next_page_params:
        total_results_fetched += num_results
        if total_results_fetched < max_results:
          kargs.update(next_page_params)
        else:
          fetch_more_data = False
      else:
        fetch_more_data= False
    return response

  # we can do more things such as pages here
  return _call
