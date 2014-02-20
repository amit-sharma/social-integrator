'''
A bunch of definition within a class to call Last.fm api.
'''

from socintpy.caller.caller import call
from socintpy.caller.api_call_error import APICallError
from api_caller import APICaller
from base_caller import BaseCaller
import json
from pprint import pprint
import logging

class LastfmAPI(BaseCaller):
  def __init__(self, kwargs):
    #auth_handler=None, host='ws.audioscrobbler.com',
    #           api_root='/', api_key=None, cache=None, secure=False,
    #           retry_count=0, retry_delay=0, retry_errors=None, api_delay=None):
    super(LastfmAPI, self).__init__(kwargs)
    #auth_handler, host, api_root, cache, 
    #                                secure, retry_count, retry_delay,
    #                                retry_errors, api_delay)
    self.irrelevant_keys = ["image", "realname"]

  get_data = call(
      APICaller(),
      path='2.0/',
      allowed_param=[]
      )
  # Assuming that getInfo and getFriends never return None. That is,
  # there is no error in fetching them. 
  def get_node_info(self, user):
    # Error handling is such that None means an error, and [] means zero values.
    returned_data = self.call_multiple_methods(user, self.node_info_calls)

    datadict = {}
    if 'user.getInfo' in returned_data:
      infodict = json.loads(returned_data['user.getInfo'])
      self.sanitizeKeys(infodict['user'])
      self.process_fields(infodict['user'])
      datadict.update(infodict['user'])
    if 'user.getRecentTracks' in returned_data and returned_data['user.getRecentTracks'] is not None:
      datadict['tracks'] = self.get_tracks_from_json(returned_data['user.getRecentTracks'], category="recenttracks")
    if 'user.getLovedTracks' in returned_data and returned_data['user.getLovedTracks'] is not None:
      datadict['lovedtracks'] = self.get_tracks_from_json(returned_data['user.getLovedTracks'], category="lovedtracks")

    if 'user.getBannedTracks' in returned_data and returned_data['user.getBannedTracks'] is not None:
      datadict['bannedtracks'] = self.get_tracks_from_json(returned_data['user.getBannedTracks'], category="bannedtracks")


    #returned_data = self.get_data(user=user, method=method, api_key=self.api_key)
    return datadict

  def get_edges_info(self, user):
    returned_data = self.call_multiple_methods(user, self.edge_info_calls)
    datadict = {}

    # this API method was called and there was no exception in its execution
    if 'user.getFriends' in returned_data and returned_data['user.getFriends'] is not None:
      """
      if 'total' in datadict['friends'] and datadict['friends']['total'] == "0":
        return None
      """  
      friends_list = self.get_tracks_from_json(returned_data['user.getFriends'], category="friends", datatype="user")

      for friend_info in friends_list:
        #self.sanitizeKeys(friend_info)
        friend_info['source'] = user
        friend_info['target'] = friend_info['name']
        #self.process_fields(friend_info)
      datadict['friends'] = friends_list
    return datadict['friends']

  def process_fields(self, datadict):
    for key in self.irrelevant_keys:
      if key in datadict:
        del datadict[key]
    return datadict
  
  def get_default_params(self, method_name):
    params = {}
    params['format'] = self.response_format
    params['api_key'] = self.api_key
    if method_name in self.method_default_params:
      params.update(self.method_default_params[method_name])
    return params

  def get_tracks_from_json(self, jsondata, category, datatype="track"):
    trackslist = None
    tracksdict = json.loads(jsondata)
    #print tracksdict
    if 'total' in tracksdict[category] and tracksdict[category]['total'] == "0":
      trackslist = []
    else:
      if type(tracksdict[category][datatype]) is dict:
        tracksdict[category][datatype] = [tracksdict[category][datatype]]
      for track in tracksdict[category][datatype]:
        self.sanitizeKeys(track)
        self.process_fields(track)
      trackslist = tracksdict[category][datatype]
    return trackslist

  @staticmethod
  def analyze_page(resp, method):
    #print resp
    resp_dict = json.loads(resp)
    """if 'error' in resp_dict:
      error_str = "Error fetching %s because:" %(method, resp_dict['message'])
      print error_str
      logging.error(error_str)
      
    pprint(resp_dict)
    """
    next_page_params = {}
    num_items = None
    if method == "user.getRecentTracks":
      if '@attr' in resp_dict['recenttracks']:
        attr_dict = resp_dict['recenttracks']['@attr']
        if int(attr_dict['totalPages']) - int(attr_dict['page']) > 0:
          next_page_params = {'page': int(attr_dict['page']) + 1}
        num_items = int(attr_dict['perPage'])
    
    return next_page_params, num_items 
  
  def is_error(self, resp_str, method):
    resp_dict = json.loads(resp_str)
    call_error = False
    if len(resp_str) < 1 or not resp_dict:
      call_error = True
      error_str = "Error fetching %s because server returned empty string." %method
    elif 'error' in resp_dict:
      call_error = True
      error_str = "Error fetching %s because: %s" %(method, resp_dict['message'
])  
    if call_error: 
      print error_str
      logging.error(error_str)
    
    return call_error

