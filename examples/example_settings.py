LABEL = "lastfm"
API_KEY = "<YOUR_API_KEY>"
HOST = "ws.audioscrobbler.com"
API_ROOT = "/"
RETRY_COUNT = 0
RETRY_DELAY = 0
API_DELAY = 1
LOG_LEVEL = "DEBUG"
RESPONSE_FORMAT = "json"
NODE_INFO_CALLS = ["user.getInfo", "user.getRecentTracks"]
EDGE_INFO_CALLS = ["getFriends"]

METHOD_DEFAULT_PARAMS = {
                         'user.getInfo':  {},
                         'user.getRecentTracks': {'limit': 200}
                        }
