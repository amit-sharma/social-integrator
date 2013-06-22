
from socintpy.caller.lastfm_api import LastfmAPI

api = LastfmAPI()

result = api.get_user_info(user='amit_ontop', method="user.getinfo" 
)
print result

result = api.get_friends(user='amit_ontop', method='user.getfriends')
print result

