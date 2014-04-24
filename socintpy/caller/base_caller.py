from socintpy.caller.api_call_error import APICallError


class BaseCaller(object):
    def __init__(self, kwargs):
        #label, auth_handler, host, api_root, cache, secure,
        #           retry_count, retry_delay, retry_errors, api_delay):
        self.label = kwargs.get('label', None)
        self.auth = kwargs.get('auth_handler', None)
        self.host = kwargs.get('host', None)
        self.api_root = kwargs.get('api_root', None)
        self.cache = kwargs.get('cache', None)
        self.secure = kwargs.get('secure', None)
        self.retry_count = kwargs.get('retry_count', None)
        self.retry_delay = kwargs.get('retry_delay', None)
        self.retry_errors = kwargs.get('retry_errors', None)
        self.response_format = kwargs.get('response_format', None)
        self.api_delay = kwargs.get('api_delay', None)
        self.api_key = kwargs.get('api_key', None)
        self.node_info_calls = kwargs.get('node_info_calls', None)
        self.edge_info_calls = kwargs.get('edge_info_calls', None)
        self.previous_call_time = 0
        self.method_default_params = kwargs.get('method_default_params', None)
        self.errorcodes_dict = kwargs.get('errorcodes_dict', None)

    def call_multiple_methods(self, user, methodnames_array):
        response = {}
        for method_name in methodnames_array:
            params = {
                'user': user,
                'method': method_name,
                'api_key': self.api_key,
                'format': self.response_format,
            }
            try:
                response[method_name] = self.get_data(**params)
            except APICallError, e:
                response[method_name] = {'error_code': e.error_code, 'error_message': e.error_message}
                #import pprint
        #pprint.pprint(response)
        return response

    def get_node_info():
        raise NotImplementedError

    def get_connections():
        raise NotImplementedError

    def get_edges_info():
        raise NotImplementedError

    def get_followers():
        raise NotImplementedError

    def get_followees():
        raise NotImplementedError

    def get_favorites():
        raise NotImplementedError

    def set_last_call_time(self, timestamp):
        self.previous_call_time = timestamp
        return

    def sanitizeKeys(self, data_dict):
        if 'id' in data_dict:
            data_dict['web_id'] = data_dict['id']
        elif 'source' in data_dict:
            data_dict['web_source'] = data_dict['source']
        elif 'target' in data_dict:
            data_dict['web_target'] = data_dict['target']
