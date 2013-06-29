'''
A bunch of definition within a class to call Last.fm api.
'''

from socintpy.caller.caller import call
from api_caller import APICaller
from base_caller import BaseCaller
import json

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
      path='2.0/?user={user}&method={method}&api_key={api_key}' + '&format=json',
      allowed_param=['user','method','api_key']
      )
  
  def get_node_info(self, user, method = "user.getinfo"):
    returned_data = self.call_multiple_methods(user, self.node_info_calls,
        LastfmAPI.get_data)
    datadict = {}
    if 'user.getInfo' in returned_data:
      infodict = json.loads(returned_data['user.getInfo'])
      self.process_fields(infodict['user'])
      datadict.update(infodict['user'])
    if 'user.getRecentTracks' in returned_data:
      tracksdict = json.loads(returned_data['user.getRecentTracks'])
      for track in tracksdict['recenttracks']['track']:
        self.process_fields(track)
      datadict['tracks'] = tracksdict['recenttracks']['track']

    #print datadict

    #returned_data = self.get_data(user=user, method=method, api_key=self.api_key)
    return datadict

  def get_edges_info(self, user, method = "user.getfriends"):
    returned_data = self.get_data(user=user, method=method, api_key=self.api_key)
    datadict = json.loads(returned_data)
    friends_list = datadict['friends']['user']
    for friend_info in friends_list:
      self.sanitizeKeys(friend_info)
      friend_info['source'] = user
      friend_info['target'] = friend_info['name']
      self.process_fields(friend_info)

    return friends_list

  def process_fields(self, datadict):
    for key in self.irrelevant_keys:
      if key in datadict:
        del datadict[key]
    return datadict


