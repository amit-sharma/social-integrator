
from socintpy.caller.github_api import GithubAPI

api = GithubAPI()

result = api.get_user_info(user_id='amit-sharma')
print result

result = api.get_followers(user_id='octocat')
print result

result = api.get_followees(user_id='octocat')
print result
