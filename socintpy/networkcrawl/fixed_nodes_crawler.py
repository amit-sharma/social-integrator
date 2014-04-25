from socintpy.networkcrawl.crawler_interface import NetworkCrawlerInterface
from socintpy.store.generic_store import GenericStore
from socintpy.store.graph_buffer import GraphBuffer
import logging

class FixedNodesCrawler(NetworkCrawlerInterface):
    def __init__(self, network, store_type):
        self.network= network
        self.store_type = store_type
        store_class = GenericStore.get_store_class(store_type)
        # Buffer to store network data
        self.gbuffer = GraphBuffer(self.network.label, store_class)

    def crawl(self, nodes_list):
        # Get details about the current node
        try:
            for new_node in nodes_list:
                new_node_info = self.network.get_node_info(new_node)
                if new_node_info is None:
                    error_msg = "Crawler: Error in fetching node info. Skipping node %s" % new_node
                    logging.error(error_msg)
                    print error_msg
                    continue
                    #raise NameError
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
                new_node_info['id'] = next(node_counter)
                self.gbuffer.store_node(new_node, new_node_info)
                self.pqueue.mark_visited(new_node)
                for edge_info in new_node_edges_info:
                    edge_info['id'] = next(edge_counter)
                    self.gbuffer.store_edge(str(edge_info['id']), edge_info)
                logging.info("Processed %s \n" % new_node)
                print "Processed ", new_node

        except (KeyboardInterrupt, SystemExit):
            self.pqueue.close()
            self.gbuffer.close()


