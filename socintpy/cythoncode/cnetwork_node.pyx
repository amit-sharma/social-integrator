import cython
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdio cimport printf
from libc.math cimport sqrt
#from libc.stdlib import qsort
import scipy.sparse as sp
import numpy as np
cimport numpy as np
cimport cython


cdef extern from "stdlib.h":
    void qsort(void *base, size_t nmemb, size_t size, int (const void *, const void *))

DTYPE = np.float
ctypedef np.float_t DTYPE_t

DTYPE_INT = np.int
ctypedef np.int_t DTYPE_INT_t

cdef struct idata:
    int item_id
    char *timestamp
    int rating

cdef struct fdata:
    int receiver_id

cpdef compute_global_topk_similarity(all_nodes, interact_type, klim):
    cdef CNetworkNode c_node_obj
    cdef int total_interactions = 0
    cdef int total_nodes = 0
    for node_obj in all_nodes:
        c_node_obj = <CNetworkNode>node_obj
        total_interactions += c_node_obj.c_length_list[interact_type]
        total_nodes += 1

    cdef np.ndarray[DTYPE_t, ndim=1] data = np.empty(total_interactions, dtype=DTYPE)
    cdef np.ndarray[DTYPE_INT_t, ndim=1] indices = np.empty(total_interactions, dtype=DTYPE_INT)
    cdef np.ndarray[DTYPE_INT_t, ndim=1] indptr = np.empty(total_nodes+1+1, dtype=DTYPE_INT)
    cdef int curr_index = 0
    cdef int i,j
    i = 0
    indptr[0:2]=0
    i+=1
    for node_obj in all_nodes:
        c_node_obj = <CNetworkNode>node_obj
        i+=1
        for j in range(c_node_obj.c_length_list[interact_type]):
            data[curr_index] = 1
            indices[curr_index] = c_node_obj.c_list[interact_type][j].item_id
            curr_index += 1
        indptr[i] = indptr[i-1] + c_node_obj.c_length_list[interact_type]

    mat = sp.csr_matrix((data, indices, indptr))
    #cc = mat * mat.transpose()
    #print mat[6,346165], mat[1,1], mat[1,1669118]
    #print data.shape[0]
    #print mat.shape[0], mat.shape[1]
    #print cc.shape[0], cc.shape[1]
    return mat


cdef int comp_interactions(const void *elem1, const void *elem2):
    cdef idata *a
    cdef idata *b
    a = <idata *> elem1
    b = <idata *> elem2
    if a[0].item_id > b[0].item_id: return 1
    if a[0].item_id < b[0].item_id: return -1
    return 0

cdef int comp_friends(const void *elem1, const void *elem2):
    cdef fdata *a
    cdef fdata *b
    a = <fdata *> elem1
    b = <fdata *> elem2
    if a[0].receiver_id > b[0].receiver_id: return 1
    if a[0].receiver_id < b[0].receiver_id: return -1
    return 0

cdef float l2_norm_dict(dict_val_iter):
    cdef float norm = 0
    cdef float temp
    for v in dict_val_iter:
        temp = len(v)
        norm += temp*temp
    norm = sqrt(norm)
    return norm

cdef int exists(int val, fdata *arr, int length_arr):
    cdef int low, high, mid
    low = 0
    high = length_arr -1

    while(low <= high):
        mid = (low + high)/2
        if val == arr[mid].receiver_id:
            return 1
        elif val > arr[mid].receiver_id:
            low = mid+1
        else:
            high = mid-1
    return 0

@cython.boundscheck(False)
cdef DTYPE_t min(np.ndarray[DTYPE_t, ndim=1] arr, int length_arr, int *min_index):
    cdef int i
    cdef DTYPE_t min_val = arr[0]
    min_index[0] = 0
    for i in range(1, length_arr):
        if arr[i] < min_val:
            min_val = arr[i]
            min_index[0] = i
    return min_val

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
        self.c_should_have_friends = kwargs['should_have_friends']
        self.c_should_have_interactions = kwargs['should_have_interactions']

    def __dealloc__(self):
        cdef int interact_type
        for interact_type in xrange(self.c_num_interact_types):
            PyMem_Free(self.c_list[interact_type])
        PyMem_Free(self.c_list)
        PyMem_Free(self.c_length_list)
        PyMem_Free(self.c_friend_list)


    cpdef int store_interactions(self, interact_type, ilist):
        self.c_list[interact_type] = <idata *>PyMem_Malloc(len(ilist)*cython.sizeof(idata))
        if self.c_list is NULL:
            raise MemoryError()
        cdef int i
        for i in range(len(ilist)):
            #print "In the loop %d" %i
            self.c_list[interact_type][i].item_id = ilist[i].item_id
            self.c_list[interact_type][i].rating = ilist[i].rating
            self.c_list[interact_type][i].timestamp = ilist[i].timestamp
        self.c_length_list[interact_type] = len(ilist)
        qsort(self.c_list[interact_type], self.c_length_list[interact_type], sizeof(idata), comp_interactions)
        return self.c_length_list[interact_type]

    cpdef int store_friends(self, flist):
        self.c_friend_list = <fdata *>PyMem_Malloc(len(flist)*cython.sizeof(fdata))
        if self.c_friend_list is NULL:
            raise MemoryError()
        cdef int i
        for i in xrange(len(flist)):
            self.c_friend_list[i].receiver_id = flist[i].receiver_id
        self.c_length_friend_list = len(flist)
        
        qsort(self.c_friend_list, self.c_length_friend_list, sizeof(fdata), comp_friends)
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
        for i in range(self.c_length_list[interact_type]):                 
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

    cpdef compute_node_similarity(self, others_interactions, int interact_type):
        cdef int length_my_interactions
        cdef idata *my_interactions
        my_interactions = self.c_list[interact_type]
        length_my_interactions = self.c_length_list[interact_type]
        cdef float simscore = 0
        cdef float l2_norm1, l2_norm2
        cdef int i, item_id
        for i in range(length_my_interactions):
            item_id = my_interactions[i].item_id
            if item_id in others_interactions:
                simscore += 1
        l2_norm1 = sqrt(length_my_interactions)
        l2_norm2 = sqrt(len(others_interactions))
        return simscore/(l2_norm1*l2_norm2)

    cdef compute_node_similarity_c(self, idata *others_interactions, int length_others_interactions, int interact_type):
        cdef float l2_norm1, l2_norm2, simscore
        cdef idata *my_interactions = self.c_list[interact_type]
        cdef int i, j
        simscore = 0
        i = 0 
        j = 0
        while i < self.c_length_list[interact_type] and j < length_others_interactions:
            if my_interactions[i].item_id < others_interactions[j].item_id:
                i += 1
            elif my_interactions[i].item_id > others_interactions[j].item_id:
                j+= 1
            else:
                simscore +=1
                i += 1
                j += 1
        l2_norm1 = sqrt(self.c_length_list[interact_type])
        l2_norm2 = sqrt(length_others_interactions)

        return simscore/(l2_norm1*l2_norm2)

    @cython.boundscheck(False)
    cpdef compute_global_topk_similarity(self, allnodes_iterable, int interact_type, int klim, mat):
        x = mat[self.uid,]
        cc = mat*x.T
        cdef np.ndarray[DTYPE_t, ndim=1] sims_vector = np.zeros(klim, dtype=DTYPE)
        """
        cdef fdata *friend_arr = self.c_friend_list
        cdef int i, min_sim_index
        cdef DTYPE_t sim, min_sim
        cdef CNetworkNode c_node_obj
        min_sim = 0
        min_sim_index = 0
        for node_obj in allnodes_iterable:
            c_node_obj = <CNetworkNode>node_obj
            if (not exists(c_node_obj.c_uid, friend_arr, self.c_length_friend_list)) and c_node_obj.c_uid != self.c_uid:
                sim = self.compute_node_similarity_c(c_node_obj.c_list[interact_type], c_node_obj.c_length_list[interact_type], interact_type)
                #sim=1
                if sim > min_sim:
                    sims_vector[min_sim_index] = sim
                    min_sim = min(sims_vector, klim, &min_sim_index)
        """
        return sims_vector


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


"""
cpdef compute_global_topk_similarity(all_nodes, interact_type, klim):
    cdef CNetworkNode c_node_obj
    cdef int total_interactions = 0
    cdef int total_nodes = 0
    for node_obj in all_nodes:
        c_node_obj = <CNetworkNode>node_obj
        total_interactions += c_node_obj.c_length_list[interact_type]
        total_nodes += 1

    cdef np.ndarray[DTYPE_t, ndim=1] data = np.empty(total_interactions, dtype=DTYPE)
    cdef np.ndarray[DTYPE_INT_t, ndim=1] indices = np.empty(total_interactions, dtype=DTYPE_INT)
    cdef np.ndarray[DTYPE_INT_t, ndim=1] indptr = np.empty(total_nodes+1+1, dtype=DTYPE_INT)
    cdef int curr_index = 0
    cdef int i,j
    i = 0
    indptr[0:2]=0
    i+=1
    for node_obj in all_nodes:
        c_node_obj = <CNetworkNode>node_obj
        i+=1
        for j in range(c_node_obj.c_length_list[interact_type]):
            data[curr_index] = 1
            indices[curr_index] = c_node_obj.c_list[interact_type][j].item_id
            curr_index += 1
        indptr[i] = indptr[i-1] + c_node_obj.c_length_list[interact_type]

    mat = sp.csr_matrix((data, indices, indptr))
    cc = mat * mat.transpose()
    print mat[6,346165], mat[1,1], mat[1,1669118]
    print data.shape[0]
    print mat.shape[0], mat.shape[1]
    print cc.shape[0], cc.shape[1]
    print cc
"""