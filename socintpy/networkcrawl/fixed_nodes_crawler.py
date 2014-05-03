""" A crawler that takes in a fixed list of nodes and crawls them.

"""

from socintpy.networkcrawl.crawler_interface import NetworkCrawlerInterface
from socintpy.store.generic_store import GenericStore
from socintpy.store.graph_buffer import GraphBuffer
import logging, itertools
from socintpy.util.utils import run_async


class FixedNodesCrawler(NetworkCrawlerInterface):
    def __init__(self, network, store_type):
        self.network= network
        self.store_type = store_type
        store_class = GenericStore.get_store_class(store_type)
        # Buffer to store network data
        self.gbuffer = GraphBuffer(self.network.label, store_class)

    def crawl(self, nodes_list, start_from_index=1):
        try:
            counter = 0
            for new_node in nodes_list:
                counter +=1
                if counter < start_from_index:
                    continue

                new_node_info = self.network.get_node_info(new_node)
                if new_node_info is None:
                    error_msg = "Crawler: Error in fetching node info. Skipping node %s" % new_node
                    logging.error(error_msg)
                    print error_msg
                    continue
                # Assume dict output for all stores.
                new_node_edges_info = self.network.get_edges_info(new_node)
                if new_node_edges_info is None:
                    error_msg = "Crawler: Error in fetching node edges info. Skipping node %s" % new_node
                    logging.error(error_msg)
                    print error_msg
                    continue
                    #raise NameError

                logging.info("Crawler: Got information about %s" % new_node)

                print "Got all data"

                # Now storing the data about the node and its edges
                print "Starting to store node info"
                FixedNodesCrawler.store_data(self.gbuffer, new_node, new_node_info, new_node_edges_info)
        except (KeyboardInterrupt, SystemExit):
            self.close()

        self.close()

    def __del__(self):
        self.close()

    def close(self):
        self.gbuffer.close()

    @classmethod
    def store_data(cls, gbuffer, node_id, node_data, edges_data):
        node_data['id'] = -1
        gbuffer.store_node(node_id, node_data)

        edge_keys = []
        for edge_info in edges_data:
            edge_info['id'] = -1
            edge_keys.append(str(edge_info['source']+'_'+edge_info['target']))

        gbuffer.store_edges(edge_keys, edges_data)
        logging.info("Processed and stored successfully %s \n" % node_id)
        print "Processed and stored", node_id



