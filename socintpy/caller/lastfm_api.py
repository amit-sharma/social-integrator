'''
A bunch of definition within a class to call Last.fm api.
'''

from socintpy.caller.caller import call
from socintpy.caller.api_call_error import APICallError
from socintpy.caller import api_error_codes
from api_caller import APICaller
from base_caller import BaseCaller
import json
from pprint import pprint
import logging
import random


class LastfmAPI(BaseCaller):
    methodkey_dict = {'user.getRecentTracks': 'recenttracks',
                      'user.getLovedTracks': 'lovedtracks',
                      'user.getBannedTracks': 'bannedtracks',
                      'user.getFriends': 'friends'
    }

    def __init__(self, kwargs):
        #auth_handler=None, host='ws.audioscrobbler.com',
        #           api_root='/', api_key=None, cache=None, secure=False,
        #           retry_count=0, retry_delay=0, retry_errors=None, api_delay=None):
        super(LastfmAPI, self).__init__(kwargs)
        #auth_handler, host, api_root, cache,
        #                                secure, retry_count, retry_delay,
        #                                retry_errors, api_delay)
        self.irrelevant_keys = ["image", "realname"]


    get_data = call(
        APICaller(),
        path='2.0/',
        allowed_param=[]
    )
    # Assuming that getInfo and getFriends never return None. That is,
    # there is no error in fetching them.
    def get_node_info(self, user):
        # Error handling is such that None means an error, and [] means zero values.
        returned_data = self.call_multiple_methods(user, self.node_info_calls, abort_if_error=True)
        for val in returned_data.values():
            if 'error_code' in val:
                return None

        datadict = {}
        if 'user.getInfo' in returned_data:
            assert len(returned_data['user.getInfo']) == 1
            infodict = json.loads(returned_data['user.getInfo'][0])
            self.sanitizeKeys(infodict['user'])
            self.process_fields(infodict['user'])
            datadict.update(infodict['user'])

        if 'user.getRecentTracks' in returned_data:
            datadict['tracks'] = self.get_tracks_from_json(returned_data['user.getRecentTracks'],
                                                           category="recenttracks")
        if 'user.getLovedTracks' in returned_data:
            datadict['lovedtracks'] = self.get_tracks_from_json(returned_data['user.getLovedTracks'],
                                                                category="lovedtracks")

        if 'user.getBannedTracks' in returned_data:
            datadict['bannedtracks'] = self.get_tracks_from_json(returned_data['user.getBannedTracks'],
                                                                 category="bannedtracks")


        #returned_data = self.get_data(user=user, method=method, api_key=self.api_key)
        return datadict

    def get_edges_info(self, user):
        returned_data = self.call_multiple_methods(user, self.edge_info_calls, abort_if_error=True)
        for val in returned_data.values():
            if 'error_code' in val:
                return None

        datadict = {}

        # checking if this API method was called
        if 'user.getFriends' in returned_data:
            """
            if 'total' in datadict['friends'] and datadict['friends']['total'] == "0":
              return None
            """
            friends_list = self.get_tracks_from_json(returned_data['user.getFriends'], category="friends",
                                                     datatype="user")

            for friend_info in friends_list:
                #self.sanitizeKeys(friend_info)
                friend_info['source'] = user
                friend_info['target'] = friend_info['name']
                #self.process_fields(friend_info)
            datadict['friends'] = friends_list
        return datadict['friends']

    def process_fields(self, datadict):
        for key in self.irrelevant_keys:
            if key in datadict:
                del datadict[key]
        return datadict

    def get_default_params(self, method_name):
        params = {}
        params['format'] = self.response_format
        params['api_key'] = self.api_key
        if method_name in self.method_default_params:
            params.update(self.method_default_params[method_name])
        return params

    def get_tracks_from_json(self, jsondata_arr, category, datatype="track"):
        trackslist = None
        for jsondata in jsondata_arr:
            tracksdict = json.loads(jsondata)
            #print tracksdict
            if 'total' in tracksdict[category] and tracksdict[category]['total'] == "0":
                trackslist = []
                assert len(jsondata_arr) == 1
                break
            else:
                if type(tracksdict[category][datatype]) is dict:
                    tracksdict[category][datatype] = [tracksdict[category][datatype]]
                for track in tracksdict[category][datatype]:
                    self.sanitizeKeys(track)
                    self.process_fields(track)
                if trackslist is None:
                    trackslist = tracksdict[category][datatype]
                else:
                    trackslist.extend(tracksdict[category][datatype])
        return trackslist

    def get_uniform_random_nodes(self, n):
        """ Function to get uniformly sampled nodes.

            Chose 25M as on 04-01-2010, the number of users was slightly more than
            25M. See user 'carolinenovinha'.
            So taking users before 2010 and seeing their activity in 2014, in the hopes that their friend circle has stabilized by then.
        """
        return [str(val) for val in random.sample(xrange(1, 25000000), n)]

    @classmethod
    def analyze_page(cls, resp, method):
        """ Function to check if more pages need to be fetched.

        Assumes that error checking has already been done and thus the string given is a valid json object.
        """
        resp_dict = json.loads(resp)
        next_page_params = {}
        num_items = None
        if method == "user.getInfo":
            pass
        else:
            #logging.debug(str(resp_dict) +str(type(resp_dict))+methodkey_dict[method])
            #logging.debug(resp_dict)
            if '@attr' in resp_dict[cls.methodkey_dict[method]]:
                print method
                attr_dict = resp_dict[cls.methodkey_dict[method]]['@attr']
                if int(attr_dict['totalPages']) - int(attr_dict['page']) > 0:
                    next_page_params = {'page': int(attr_dict['page']) + 1}
                num_items = int(attr_dict['perPage'])
                print next_page_params, num_items

        return next_page_params, num_items

    def check_valid_response(self, respdict, apimethod):
        call_error = 0
        error_str = "No error found."
        if apimethod in self.methodkey_dict:
            datadict = respdict[self.methodkey_dict[apimethod]]
            if "#text" in datadict and datadict["#text"] == "\n":
                if not ("total" in datadict and int(datadict["total"]) == 0):
                    call_error = api_error_codes.MALFORMED_API_RESPONSE
                    error_str = "Error fetching %s because:: Error %d: Server returned malformed data or incomplete data." % (
                    apimethod, call_error)
        return call_error, error_str

    def is_error(self, resp_str, apimethod):
        """ API domain specific function to check for errors in the API response returned.

         If there is an error found, then APICaller will retry fetching the data via its execute method
         (if retry is enabled in settings.py).
        """
        call_error = 0
        error_str = "No error found."
        #print len(resp_str.strip('" '))
        if len(resp_str) < 1 or len(resp_str.strip('" ')) <= 1:
            call_error = api_error_codes.EMPTY_API_RESPONSE
            error_str = "Error fetching %s because:: Error %d: Server returned empty string." % (apimethod, call_error)
        else:
            resp_dict = None
            try:
                resp_dict = json.loads(resp_str)
            except ValueError, e:
                call_error = api_error_codes.MALFORMED_API_RESPONSE
                error_str = "Error fetching %s because:: Error %d: Server returned string that is not valid JSON." % (
                apimethod, call_error)

            if resp_dict is not None and call_error == 0:
                if 'error' in resp_dict:
                    call_error = self.errorcodes_dict[int(resp_dict['error'])]
                    error_str = "Error fetching %s because:: Error %d: %s" % (
                    apimethod, call_error, resp_dict['message'])
                else:
                    ce, es = self.check_valid_response(resp_dict, apimethod)
                    if ce != 0:
                        call_error = ce
                        error_str = es

        if call_error != 0:
            #print error_str
            logging.error(error_str)

        return call_error, error_str

