mkdir -p /home/as2447/datasets/lastfm_oct14/part$1
python ~/code/social-integrator/examples/crawl/lastfm_fixedcrawler.py -n /home/as2447/code/nodes_list/nodes_to_crawl_part$1 -s $2 -d /home/as2447/datasets/lastfm_oct14/part$1/ -a $3 || { uname -n; echo "Execution not done for part " $1; } | mail -s "Server status is not good" amit.ontop+server@gmail.com
{ uname -n; echo "Execution done for part " $1; } | mail -s "Server status for data collection" amit.ontop+server@gmail.com 
