from socintpy.caller.get_api import get_api, get_args
from socintpy.networkcrawl.bfs_networkcrawler import BFSNetworkCrawler
import settings
import logging

numeric_loglevel = getattr(logging, settings.LOG_LEVEL, None)
logging.basicConfig(filename="socintpy.log", level = numeric_loglevel)
api_args = get_args(settings)
api = get_api(api_args)

# If you want to pass a dictionary (say params_dict), use **params_dict
result = api.get_data(user='amit_ontop', method="user.getRecentTracks",
    max_results = 100)                
print result      
"""
result = api.get_edges_info(user='amit_ontop', method='user.getfriends',
                         api_key = settings.LASTFM_APIKEY)                                             
print result  

crawler = BFSNetworkCrawler(api, store_type="gml")
crawler.set_seed_nodes(["amit_ontop"])
crawler.crawl(max_nodes=10)

"""
