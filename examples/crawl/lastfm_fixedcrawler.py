""" Example file to demonstrate use of the API.

"""
import getopt
from socintpy.caller.get_api import get_api, get_args
from socintpy.networkcrawl.fixed_nodes_crawler import FixedNodesCrawler
import examples.settings as settings  # Local settings file, not in repo for security reasons
import logging, sys

def usage():
    print "Too few or erroneous parameters"
    print 'Usage: python '+sys.argv[0]+' -n <input-file> -s <start_from_index> -d <database-directory> -a <api-key>'

if __name__ == "__main__":

    try:
        opts, args = getopt.getopt(sys.argv[1:], "n:s:d:a:", ["help", "output="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    if not opts:
        usage()
        sys.exit(2)

    start_from_index = None
    data_dir = None
    cmd_api_key = None
    for o,a in opts:
        if o=="-n":
            nodes_list = []
            with open(a) as nodes_file:
                for line in nodes_file:
                    node_id = line.strip("\n ")
                    nodes_list.append(node_id)
        if o=="-s":
            start_from_index = int(a)
        if o == "-d":
            data_dir = a
        if o == "-a":
            cmd_api_key = a


    # Set the log level
    numeric_loglevel = getattr(logging, settings.LOG_LEVEL, None)
    logging.basicConfig(filename=data_dir+"socintpy_fixed.log", level=numeric_loglevel)

    #cmd_params = cmd_script.get_cmd_parameters(sys.argv)
    cmd_params = {'mode':'fetch_data'}    # Fetch api object using the settings from settings.py

    api_args = get_args(settings)
    if cmd_api_key is not None:
        api_args['api_key'] = cmd_api_key
    api = get_api(api_args)



    if cmd_params['mode'] == "fetch_data":
        # Set up the data crawl
        logging.info("STARTING FETCH_DATA CRAWL. STANDBY!")
        logging.info("CRAWLER: FixedNodesCrawler")
        logging.info("STORE: sqlite")
        logging.info("START_FROM_INDEX: %d" %start_from_index)
        logging.info("DATABASE_DIRECTORY: %s" %a)
        logging.info("API_KEY: %s" %api_args['api_key'])
        print("API_KEY: %s" %api_args['api_key'])
        
        crawler = FixedNodesCrawler(api, store_type="sqlite", output_dir=data_dir)
        # Start the data crawl
        crawler.crawl(nodes_list, start_from_index=start_from_index)

    elif cmd_params['mode'] == "retry_errors":
        nodes_with_error = cmd_script.get_nodes_with_error(logfile="/fd/fd/socintpy_old.log")
        print nodes_with_error, len(nodes_with_error)
        logging.info("STARTING RETRY_ERRORS CRAWL. STANDBY!")
        crawler = FixedNodesCrawler(api, store_type="sqlite", output_dir=data_dir)
        nodes_stored, edges_stored = cmd_script.recover_num_items_stored(logfile="socinty_old.log")
