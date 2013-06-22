
from socintpy.caller.reddit_api import RedditAPI

api = RedditAPI()

result = api.link_by_fullname(id='t3_6nw57', headers={'User-Agent' : 'test-api'})
print result
