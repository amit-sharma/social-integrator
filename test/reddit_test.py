
from socintpy.caller.reddit_api import RedditAPI
API_KEY = "1a9c762edcaa85b4d18831a816ea3738"
api = RedditAPI()

result = api.link_by_fullname(id='t3_6nw57',
                              api_key=API_KEY,
                              headers={'User-Agent' : 'test-api'})
print result
