'''
A bunch of definition within a class to call Reddit api.
'''

from caller import call
from api_caller import APICaller


class RedditAPI(object):
    def __init__(self, auth_handler=None, host='www.reddit.com',
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

    link_by_fullname = call(
        APICaller(),
        path='/by_id/{id}.json',
        allowed_param=['id']
    )


