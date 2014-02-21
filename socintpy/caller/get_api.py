from socintpy.caller.lastfm_api import LastfmAPI
from socintpy.caller.reddit_api import RedditAPI

def get_api(kwargs):                                         
  if 'label' not in kwargs:
    raise KeyError("Please specify the network using the 'LABEL' key")
  if kwargs['label'] == "lastfm":
    return LastfmAPI(kwargs)                                                   
  elif kwargs['label'] == "reddit":                                                 
    return RedditAPI(kwargs)                                                   
  return None 

def get_args(settings_module):
  args = {}
  args['label'] = settings_module.LABEL
  args['api_key'] = settings_module.API_KEY
  args['api_delay'] = settings_module.API_DELAY
  args['api_root'] = settings_module.API_ROOT
  args['host'] = settings_module.HOST
  args['retry_count'] = settings_module.RETRY_COUNT
  args['retry_delay'] = settings_module.RETRY_DELAY
  args['response_format'] = settings_module.RESPONSE_FORMAT.lower()
  args['node_info_calls'] = settings_module.NODE_INFO_CALLS
  args['edge_info_calls'] = settings_module.EDGE_INFO_CALLS
  args['method_default_params'] = settings_module.METHOD_DEFAULT_PARAMS
  args['errorcodes_dict'] = settings_module.ERRORCODES_DICT
  return args
