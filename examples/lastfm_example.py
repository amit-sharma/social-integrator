""" Example file to demonstrate use of the API.

"""
from socintpy.caller.get_api import get_api, get_args
from socintpy.networkcrawl.bfs_networkcrawler import BFSNetworkCrawler
import settings  # Local settings file, not in repo for security reasons
import logging, sys
import cmd_script

if __name__ == "__main__":
    # Set the log level
    numeric_loglevel = getattr(logging, settings.LOG_LEVEL, None)
    logging.basicConfig(filename="socintpy.log", level=numeric_loglevel)

    # Fetch api object using the settings from settings.py
    api_args = get_args(settings)
    api = get_api(api_args)
    """
    # If you want to pass a dictionary (say params_dict), use **params_dict
    result = api.get_data(user='amit_ontop', method="user.getRecentTracks",
    max_results = 100)                
    print result
    """
    cmd_params = cmd_script.get_cmd_parameters(sys.argv)
    if cmd_params['mode'] == "fetch_data":
        # Set up the data crawl
        crawler = BFSNetworkCrawler(api, store_type="sqlite")

        #crawler = BFSNetworkCrawler(api, seed_nodes=None, store_type="basic_shelve", recover=True)
        # Start the data crawl
        crawler.crawl(seed_nodes=api.get_uniform_random_nodes(100), max_nodes=1000000, recover=True, checkpoint_frequency=1)
    elif cmd_params['mode'] == "retry_errors":
        nodes_with_error = cmd_script.get_nodes_with_error(logfile="socintpy_old.log")
        print nodes_with_error, len(nodes_with_error)
        #crawler = FixedNodesCrawler(api, logfile="socintpy.log", store_type="sqlite")
        #crawler.crawl()
