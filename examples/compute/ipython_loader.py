import numpy as np
import operator
from network_analyzer_example import *
import compare_adopt_share
from compare_adopt_share import *
import socintpy.util.plotter as plotter
from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer

if __name__ == "__main__":
    if len(sys.argv) ==2:
        print sys.argv[1] # if True, then use raw_data csvs and store as ego_nets
        data = get_data(bool(sys.argv[1]))
    else:
        data = get_data()
    net_analyzer = BasicNetworkAnalyzer(data)
    #net_analyzer.show_basic_stats()

na = AdoptShareComparer(data)
