import getopt, sys
from socintpy.util.priority_queue import PriorityQueue

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
    private_data_error_str = "This user has made their recent tracks private. Only they can access them with an authenticated session"
    private_data_message_charindex = len(private_data_error_str)
    private_data_error = False
    with open(logfile, "r") as f:
        for line in f:
            if line.strip("\n ")[-private_data_message_charindex:] == private_data_error_str:
                private_data_error = True
            if line[:lead_chars] == leading_str:
                if private_data_error:
                    private_data_error = False
                else:
                    node_id = line[lead_chars:].strip("\n ")
                    nodes_with_error.append(node_id)

    return nodes_with_error

def recover_num_items_stored(logfile):
    state_store_path = read_param_from_log(logfile, param_name="STATE_STORE_PATH")
    checkpoint_freq = read_param_from_log(logfile, param_name="CHECKPOINT_FREQUENCY") # in real code, this would be fetch from the log. Currently the log that we have run does not support it.
    state_store = PriorityQueue("sqlite", store_name=state_store_path)
    return state_store.initialize(seed_nodes=[], do_recovery=True, checkpoint_freq=checkpoint_freq )

def read_param_from_log(logfile, param_name):
    info_prefix = "INFO:root:"
    with open(logfile) as f:
        for line in f:
            if line[:len(info_prefix)+len(param_name)] == (info_prefix + param_name):
                param_val = line[param_name:].strip("\n ")
                break
    return param_val

