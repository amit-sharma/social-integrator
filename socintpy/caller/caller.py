'''
A class to implement general API call function.
'''

def call(caller, **config):
  '''Similar to the method in tweepy, but api_method can change
  so that we have different api methods for different websites'''

  # use config to init api caller
  caller.set_config(config)

  def _call(api, *args, **kargs):
    caller.set_up(api, args, kargs)
    return caller.execute()

  # we can do more things such as pages here
  return _call

