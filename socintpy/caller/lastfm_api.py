'''
A bunch of definition within a class to call Reddit api.
'''

from caller import call
from api_caller import APICaller
from caller_interface import CallerInterface

API_KEY = "1a9c762edcaa85b4d18831a816ea3738"
class LastfmAPI(CallerInterface):
  def __init__(self, auth_handler=None, host='ws.audioscrobbler.com',
      api_root='/', cache=None, secure=False, retry_count=0,
      retry_delay=0, retry_errors=None):
    self.auth = auth_handler
    self.host = host
    self.api_root = api_root
    self.cache = cache
    self.secure = secure
    self.retry_count = retry_count
    self.retry_delay = retry_delay
    self.retry_errors = retry_errors
    #self.api_key = "1a9c762edcaa85b4d18831a816ea3738"

  get_user_info = call(
      APICaller(),
      path='2.0/?user={user}&method={method}&api_key='+API_KEY+'&format=json',
      allowed_param=['user','method','api_key']
      )
  get_friends = call(
      APICaller(),
      path='2.0/?user={user}&method={method}&api_key='+API_KEY+'&format=json',
      allowed_param=['user', 'method', 'api_key']
      )


