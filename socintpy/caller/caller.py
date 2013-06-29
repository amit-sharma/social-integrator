'''
A class to implement general API call function.
'''
import time
import logging
def call(caller, **config):
  '''Similar to the method in tweepy, but api_method can change
  so that we have different api methods for different websites'''

  # use config to init api caller
  caller.set_config(config)

  def _call(api, *args, **kargs):
    caller.set_up(api, args, kargs)
    timediff = time.time() - api.previous_call_time
    #print timediff, api.api_delay
    if api.api_delay and timediff < api.api_delay:
      time.sleep(api.api_delay - timediff)
    logging.debug("Time between calls = %f seconds" % (time.time() -
      api.previous_call_time))
    api.set_last_call_time(time.time())
    return caller.execute()

  # we can do more things such as pages here
  return _call
