import cython
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdio cimport printf
from libc.math cimport sqrt

cdef struct idata:
    int item_id
    char *timestamp
    int rating

cdef struct fdata:
    int receiver_id

cdef float l2_norm_dict(dict_val_iter):
    cdef float norm = 0
    cdef float temp
    for v in dict_val_iter:
        temp = len(v)
        norm += temp*temp
    norm = sqrt(norm)
    return norm

@cython.freelist(1024)
@cython.no_gc_clear
cdef class CNetworkNode:
    cdef public int c_uid
    cdef public int c_should_have_friends
    cdef public int c_should_have_interactions
    cdef idata **c_list
    cdef int *c_length_list
    cdef int c_num_interact_types
    cdef fdata *c_friend_list
    cdef int c_length_friend_list

    def __cinit__(self, *args,  **kwargs):
        cdef int a
        if kwargs['should_have_interactions'] and not (kwargs['node_data'] is None):
            self.c_num_interact_types = len(kwargs['node_data'].interaction_types)
            self.c_list = <idata **>PyMem_Malloc(self.c_num_interact_types*sizeof(idata *))
            self.c_length_list = <int *>PyMem_Malloc(self.c_num_interact_types*cython.sizeof(int))
            for a in xrange(self.c_num_interact_types):
                self.c_length_list[a] = 0
        else:
            self.c_num_interact_types = 0
            self.c_list = NULL
            self.c_length_list = NULL

        self.c_friend_list = NULL
        self.c_length_friend_list = 0
        #if kwargs['should_have_friends']:
        #    self.c_friend_list = <fdata **>PyMem_Malloc(self.)
    
    def __init__(self,*args, **kwargs):
        self.c_uid = int(args[0])
        self.c_should_have_friends = int(args[1])
        self.c_should_have_interactions = int(args[2])

    def __dealloc__(self):
        PyMem_Free(self.c_list)


    cpdef int store_interactions(self, interact_type, ilist):
        self.c_list[interact_type] = <idata *>PyMem_Malloc(len(ilist)*cython.sizeof(idata))
        if self.c_list is NULL:
            raise MemoryError()
        cdef int i
        for i in xrange(len(ilist)):
            #print "In the loop %d" %i
            self.c_list[interact_type][i].item_id = ilist[i].item_id
            self.c_list[interact_type][i].rating = ilist[i].rating
            self.c_list[interact_type][i].timestamp = ilist[i].timestamp
        self.c_length_list[interact_type] = len(ilist)
        return self.c_length_list[interact_type]

    cpdef int store_friends(self, flist):
        self.c_friend_list = <fdata *>PyMem_Malloc(len(flist)*cython.sizeof(fdata))
        if self.c_friend_list is NULL:
            raise MemoryError()
        cdef int i
        for i in xrange(len(flist)):
            self.c_friend_list[i].receiver_id = flist[i].receiver_id
        self.c_length_friend_list = len(flist)
        return self.c_length_friend_list

    cpdef get_all_items_interacted_with(self):
        interacted_items = set()                                                
        cdef int i
        for curr_interact_type in xrange(self.c_num_interact_types):        
            for i in xrange(self.c_length_list[curr_interact_type]):        
                #print self.c_list[curr_interact_type][i].item_id           
                interacted_items.add(self.c_list[curr_interact_type][i].item_id)
        return interacted_items
    
    cpdef get_items_interacted_with(self, interact_type):
        interacted_items = set()
        cdef int i
        for i in xrange(self.c_length_list[interact_type]):                 
            interacted_items.add(self.c_list[interact_type][i].item_id)   
        return interacted_items

    cpdef get_friend_ids(self):
        cdef int i
        id_list = []
        for i in xrange(self.c_length_friend_list):
            id_list.append(self.c_friend_list[i].receiver_id)
        return id_list

    cpdef get_num_interactions(self, interact_type):
        if self.c_list is NULL:
            return 0
        else:
            return self.c_length_list[interact_type]

    cpdef has_interactions(self, interact_type):
        return (not (self.c_list is NULL)) and (self.c_length_list[interact_type] > 0)

    cpdef get_num_friends(self):
        if self.c_friend_list is NULL:
            return 0
        else:
            return self.c_length_friend_list

    cpdef has_friends(self):
        return (not(self.c_friend_list is NULL)) and (self.c_length_friend_list > 0)

    cpdef compute_similarity(self, items, interact_type):
        if self.c_length_list[interact_type] ==0 or len(items) == 0:
            return None 
        cdef float simscore = 0
        cdef float l2_norm1, l2_norm2
        cdef int i, item_id
        for i in xrange(self.c_length_list[interact_type]):
            item_id = self.c_list[interact_type][i].item_id
            if item_id in items:
                simscore += len(items[item_id])
        l2_norm1 = sqrt(self.c_length_list[interact_type])
        l2_norm2 = l2_norm_dict(items.itervalues())
        return simscore/(l2_norm1*l2_norm2)

    property uid:
        def __get__(self):
            return self.c_uid

    property should_have_interactions:
        def __get__(self):
            return self.c_should_have_interactions

    property should_have_friends:
        def __get__(self):
            return self.c_should_have_friends

    #property interactions:
    #    def __get__(self):
    #        return self.c_list
