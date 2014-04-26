from socintpy.networkcompute.basic_network_analyzer import BasicNetworkAnalyzer
from socintpy.networkdata.hashtag_data_preparser import HashtagDataPreparser

data = HashtagDataPreparser("/home/asharma/datasets/twitter_jure/")
data.get_all_data()
net_analyzer = BasicNetworkAnalyzer(data)
net_analyzer.show_basic_stats()