'''
A bunch of definition within a class to call Reddit api.
'''

from caller import call
from api_caller import APICaller
from caller_interface import CallerInterface
class GithubAPI(CallerInterface):
  def __init__(self, auth_handler=None, host='api.github.com',
      api_root='/', cache=None, secure=True, retry_count=0,
      retry_delay=0, retry_errors=None):
    self.auth = auth_handler
    self.host = host
    self.api_root = api_root
    self.cache = cache
    self.secure = secure
    self.retry_count = retry_count
    self.retry_delay = retry_delay
    self.retry_errors = retry_errors

  get_user_info = call(
      APICaller(),
      path='users/{user_id}',
      allowed_param=['user_id']
      )
  get_followers = call(
      APICaller(),
      path='users/{user_id}/followers',
      allowed_param=['user_id']
      )
  get_followees = call(
      APICaller(),
      path='users/{user_id}/following',
      allowed_param=['user_id']
      )


