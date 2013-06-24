'''
A bunch of definition within a class to call Last.fm api.
'''

from caller import call
from api_caller import APICaller
from base_caller import BaseCaller

class LastfmAPI(BaseCaller):
  def __init__(self, label = "lastfm", auth_handler=None, host='ws.audioscrobbler.com',
               api_root='/', api_key=None, cache=None, secure=False,
               retry_count=0, retry_delay=0, retry_errors=None):
    super(LastfmAPI, self).__init__(label, auth_handler, host, api_root, cache, 
                                    secure, retry_count, rety_delay, retry_errors)
    self.api_key = api_key

  get_node_info = call(
      APICaller(),
      path='2.0/?user={user}&method={method}&api_key={api_key}' + '&format=json',
      allowed_param=['user','method','api_key']
      )
  get_connections = call(
      APICaller(),
      path='2.0/?user={user}&method={method}&api_key={api_key}'  + '&format=json',
      allowed_param=['user', 'method', 'api_key']
      )


