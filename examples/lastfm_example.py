""" Example file to demonstrate use of the API.

"""
from socintpy.caller.get_api import get_api, get_args
from socintpy.networkcrawl.bfs_networkcrawler import BFSNetworkCrawler
import settings # Local settings file, not in repo for security reasons
import logging

# Set the log level
numeric_loglevel = getattr(logging, settings.LOG_LEVEL, None)
logging.basicConfig(filename="socintpy.log", level = numeric_loglevel)

# Fetch api object using the settings from settings.py
api_args = get_args(settings)
api = get_api(api_args)
"""
# If you want to pass a dictionary (say params_dict), use **params_dict
result = api.get_data(user='amit_ontop', method="user.getRecentTracks",
    max_results = 100)                
print result      
"""

# Set up the data crawl
crawler = BFSNetworkCrawler(api, store_type="basic_shelve")

# Start the data crawl
crawler.crawl(seed_nodes=["jrs1991"], max_nodes = 10)

