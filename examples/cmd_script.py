import getopt, sys

def usage(cmd_name):
    print "Too few or erroneous parameters"
    print 'Usage: python '+cmd_name + "-m" + "mode (fetch_data or retry_errors)"

def get_cmd_parameters(argval_list):
    try:
        opts, args = getopt.getopt(argval_list[1:], "m:", ["help", "output_dir="])
    except getopt.GetoptError as err:
        # print help information and exit:
        # will print something like "option -a not recognized"
        usage(argval_list[0])
        sys.exit(2)
    if not opts:
        usage(argval_list[0])
        sys.exit(2)

    mode=None
    for o, a in opts:
        if o =="-m":
            mode = a

    if mode is None:
        assert False, "Invalid mode!"
    else:
        return({'mode':mode})

def get_nodes_with_error(logfile):
    nodes_with_error = []
    leading_str =  "ERROR:root:Crawler: Error in fetching node info. Skipping node "
    lead_chars = len(leading_str)
    with open(logfile, "r") as f:
        for line in f:
            if line[:lead_chars] == leading_str:
                node_id = line[lead_chars:].strip("\n ")
                nodes_with_error.append(node_id)
    return nodes_with_error
