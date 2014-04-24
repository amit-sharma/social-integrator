from socintpy.caller.lastfm_api import LastfmAPI
import test.settings

api = LastfmAPI()

result = api.get_user_info(user='amit_ontop', method="user.getinfo",
                           api_key=test.settings.LASTFM_APIKEY)
print result

result = api.get_friends(user='amit_ontop', method='user.getfriends', api_key=
test.settings.LASTFM_APIKEY)
print result

