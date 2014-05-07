import cython
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdio cimport printf

cdef struct idata:
    int item_id
    char *timestamp
    int rating

@cython.freelist(1024)
@cython.no_gc_clear
cdef class CNetworkNode:
    cdef public int c_uid
    cdef public int c_has_friends
    cdef public int c_has_interactions
    cdef idata *c_list
    cdef int count_clist

    def __cinit__(self, *args,  **kwargs):
        #self.c_uid = args[0]
        #self.c_hasfriends = args[1]
        #self.c_hasinteractions = args[2]
        #print "cinit"
        pass

    def __init__(self,*args, **kwargs):
        self.c_uid = int(args[0])
        self.c_has_friends = int(args[1])
        self.c_has_interactions = int(args[2])
        #print "iniit"

    def __dealloc__(self):
        PyMem_Free(self.c_list)


    cpdef int store_interactions(self, interact_type, ilist):
        self.c_list = <idata *>PyMem_Malloc(len(ilist)*cython.sizeof(idata))
        if self.c_list is NULL:
            raise MemoryError()
        for i in xrange(len(ilist)):
            #print "In the loop %d" %i
            self.c_list[i].item_id = ilist[i].item_id
            self.c_list[i].rating = ilist[i].rating
            self.c_list[i].timestamp = ilist[i].timestamp
        self.count_clist = len(ilist)
        return len(ilist)

    cpdef get_items_interacted_with(self):
        interacted_items = set()
        for i in xrange(self.count_clist):
            interacted_items.add(self.c_list[i].item_id)
        return interacted_items
    property uid:
        def __get__(self):
            return self.c_uid

    property has_interactions:
        def __get__(self):
            return self.c_has_interactions

    property has_friends:
        def __get__(self):
            return self.c_has_friends

    #property interactions:
    #    def __get__(self):
    #        return self.c_list
