#!python
#cython: boundscheck=True, wraparound=True, cdivision=False

cimport cython
from cpython.mem cimport PyMem_Malloc, PyMem_Free
cimport cython.parallel
from cython.parallel import parallel, prange, threadid
from cpython cimport bool

from libc.math cimport sqrt, fabs
from libc.stdlib cimport rand, abs, free, malloc
from libc.stdio cimport printf
from libc.string cimport memmove, memcpy

import numpy as np
cimport numpy as np
import scipy.sparse as sp

import sys
from itertools import izip
#cimport c_uthash

cimport socintpy.cythoncode.data_types as data_types
cimport socintpy.cythoncode.binary_search as binary_search
from data_types cimport netnodedata, idata,fdata,int_counter, nodesusceptinfo, fsiminfo
from data_types cimport UT_hash_table, UT_hash_handle, HASH_FIND_INT, HASH_ADD_INT_CUSTOM,HASH_SORT, HASH_DEL, delete_hashtable
from data_types cimport comp_interactions_temporal, comp_interactions, comp_friends, compare_int_counter,comp_fsiminfo
from binary_search cimport binary_search_standard, binary_search_closest_temporal, binary_search_closest, binary_search_timesensitive, binary_search_closest_fsiminfo
from binary_search cimport exists, in_array


cdef extern from "stdlib.h":
    int RAND_MAX

cdef extern from "stdlib.h":
    void qsort(void *base, size_t nmemb, size_t size, int (const void *, const void *)) nogil

cdef extern from "Python.h":
    object PyString_FromStringAndSize(char *, Py_ssize_t)
    char *PyString_AsString(object)


DTYPE = np.float
ctypedef np.float_t DTYPE_t

DTYPE_INT = np.int
ctypedef np.int_t DTYPE_INT_t

DTYPE_INT32 = np.int32
ctypedef np.int32_t DTYPE_INT32_t


def compute_node_susceptibility_pywrapper(my_node, other_nodes,
        int length_other_nodes,
        int interact_type, float cutoff_rating, int data_type_code, 
        int min_interactions_per_user, int time_diff, int time_scale, 
        bint allow_duplicates, bint debug=False):
    cdef int i
    cdef float ret
    cdef netnodedata **other_nodes_ndata = <netnodedata **>malloc(length_other_nodes*sizeof(netnodedata *))
    for i in range(length_other_nodes):
        other_nodes_ndata[i] = &((<CNetworkNode>other_nodes[i]).ndata)
    ret = compute_node_susceptibility((<CNetworkNode>my_node).ndata, other_nodes_ndata, 
            length_other_nodes, interact_type, cutoff_rating, data_type_code,
            min_interactions_per_user, time_diff, time_scale, allow_duplicates, debug) 
    free(other_nodes_ndata)
    return ret

def compute_node_similarity_pywrapper(node, other_node, int interact_type, 
        int data_type_code, int min_interactions_per_user, 
        int time_diff, int time_scale):
    return compute_node_similarity((<CNetworkNode>node).ndata, (<CNetworkNode>other_node).ndata, interact_type,
            data_type_code, min_interactions_per_user, time_diff, time_scale)

cdef struct loopvars:
    int counter
    int counter2
    int count_success
    int eligible_nodes_counter
    int node_fnode_counter
    int node_rnode_counter

cpdef compute_susceptibility_randomselect_cfast(all_nodes_list, int interact_type, 
                                            float cutoff_rating, float control_divider, 
                                            int min_interactions_per_user, 
                                            int time_diff, int time_scale, 
                                            int max_tries, int max_node_computes,
                                            float max_interact_ratio_error,
                                            bint allow_duplicates):   
    cdef:
        int counter, counter2
        int count_success
        int eligible_nodes_counter
        int edges_counter = 0
        float max_sim_ratio_error = 0.1
        float fsim, rsim
        int num_eligible_friends
        int total_num_nodes
        int node_fnode_counter = 0
        int node_rnode_counter = 0
        int i,j
    cdef int len_nodes_arr = len(all_nodes_list)
    cdef netnodedata **allnodes = <netnodedata **>malloc(len_nodes_arr*sizeof(netnodedata *))
    cdef nodesusceptinfo node_data
    influence_arr = {}
    for i in range(1,len_nodes_arr):
        allnodes[i] = &((<CNetworkNode>all_nodes_list[i]).ndata)
        i+= 1
    total_num_nodes = i # this is actually number of nodes +1 
    counter = 0
    counter2 = 0
    cdef loopvars *buf
    cdef netnodedata *loc_allnodes
    cdef int tid = 0
    with nogil, parallel(num_threads=2):
        buf = <loopvars *>malloc(1*sizeof(loopvars))
        buf[0].counter = 0
        buf[0].counter2 = 0
        buf[0].eligible_nodes_counter = 0
        buf[0].count_success = 0
        buf[0].node_fnode_counter = 0
        buf[0].node_rnode_counter = 0
        for i in prange(1,len_nodes_arr,schedule='dynamic'):
        #for i in range(1,len_nodes_arr):
            one_nodeinfo = (<netnodedata *>allnodes[i])[0]
            if one_nodeinfo.c_list is not NULL and one_nodeinfo.c_friend_list is not NULL:
                node_data = process_one_node_susceptibility(allnodes, one_nodeinfo, interact_type, 
                                            cutoff_rating, control_divider, 
                                            min_interactions_per_user, 
                                            time_diff, time_scale, 
                                            max_tries, 
                                            max_interact_ratio_error, allow_duplicates,
                                            total_num_nodes, &(buf[0].eligible_nodes_counter),
                                            &(buf[0].node_fnode_counter), &(buf[0].node_rnode_counter))
                 
                if node_data.fsim > -0.5:# !=-1 but equality not well-defined for float
                    buf[0].count_success +=1
                     
                    with gil:
                        influence_arr[one_nodeinfo.c_uid] = (0, 0, 0, 
                           node_data.fsim, node_data.rsim,node_data.fsusc, node_data.rsusc)
                        #printf("%d: %f", one_nodeinfo.c_uid,node_data.fsim)#, node_data.rsim, node_data.fsusc, node_data.rsusc)
                    
                
                buf[0].counter2 += 1
                tid = threadid()
                if buf[0].counter2 %100==0:
                    printf("Thread %d: Done counter %d\n", tid, buf[0].counter2)
                if buf[0].counter2 > max_node_computes:
                    printf("%d, %d", buf[0].counter2, max_node_computes)
                    break
            buf[0].counter += 1
        printf("\n--Number of nodes I looped through: %d\n", buf[0].counter)
        printf("--Number of nodes I lod through(with interactions AND friends): %d\n", buf[0].counter2)
        printf("--Eligible nodes (with interactions > %d): %d\n" ,min_interactions_per_user, buf[0].eligible_nodes_counter)
        printf("--Total Edges from eligible nodes: %d\n", edges_counter)
        printf("--Number of  successful nodes (can find rnodes): %d\n", buf[0].count_success)
        printf("--Successful triplets:")#, len(triplet_nodes) 
        printf("--Nodes with eligible fnode influence measure: %d\n", buf[0].node_fnode_counter)
        printf("--Nodes with eligible fnode and rnode influence measure: %d\n", buf[0].node_rnode_counter)
        free(buf)
    PyMem_Free(allnodes)
    return influence_arr



cdef struct compfrnonfr:
    float fsim
    float rsim
    netnodedata fnode
    netnodedata rnode

cdef inline int int_min(int a, int b) nogil: return a if a <= b else b

cdef nodesusceptinfo process_one_node_susceptibility(netnodedata **allnodes, netnodedata c_node,
        int interact_type, float cutoff_rating, float control_divider, 
        int min_interactions_per_user, int time_diff, int time_scale, 
        int max_tries, float max_interact_ratio_error, bint allow_duplicates, 
        int total_num_nodes, int *ptr_eligible_nodes_counter,
        int *ptr_node_fnode_counter, int *ptr_node_rnode_counter) nogil:
    cdef nodesusceptinfo node_data
    node_data.fsim = -1
    
    if c_node.c_length_train_ids < min_interactions_per_user or c_node.c_length_test_ids <min_interactions_per_user or c_node.c_length_friend_list == 0:
        return node_data
    ptr_eligible_nodes_counter[0] += 1

    cdef:
        float min_friends_match_ratio = 0.9
        int edges_counter = 0
        float max_sim_ratio_error = 0.1
        int data_type_code=<int>'c' 
        int data_type_code2=<int>'i'
        float fsim, rsim
        int num_eligible_friends = 0
        int ictr
        int num_friends_matched, r_index, tries, f_index, r_index2
        float avg_fsim, avg_rsim, ratio_train, simdiff
        float inf1, inf2
        netnodedata  fobj
        fsiminfo *frsim_data_arr = NULL
        netnodedata **control_nonfr_nodes = NULL
        netnodedata **selected_friends = NULL
        compfrnonfr frnonfr_data
        int j
    
    frsim_data_arr = <fsiminfo *>malloc(c_node.c_length_friend_list*cython.sizeof(fsiminfo))
    edges_counter += c_node.c_length_friend_list
    ictr = 0
    
    for j in range(c_node.c_length_friend_list):
        fobj = (<netnodedata *>allnodes[c_node.c_friend_list[j].receiver_id])[0]
        #printf("%d %d \n", fobj.c_uid, c_node.c_friend_list[j].receiver_id)
        if fobj.c_length_train_ids >=min_interactions_per_user and fobj.c_length_test_ids >=min_interactions_per_user:
            fsim = compute_node_similarity(c_node, fobj, interact_type, 
                    data_type_code, 
                    min_interactions_per_user, time_diff=-1, time_scale=time_scale)
            #fsim = 1
            if fsim >-0.5:#TODO check cannot be None
                num_eligible_friends += 1
                frsim_data_arr[ictr].sim = fsim
                frsim_data_arr[ictr].uid = fobj.c_uid
                ictr += 1
    qsort(frsim_data_arr, num_eligible_friends, cython.sizeof(fsiminfo), comp_fsiminfo) 
    
    #eligible_fr_sim_arr = fr_sim_arr[:num_eligible_friends]
    max_tries = total_num_nodes #TODO change, do it in python
    #randomized_node_ids = random.sample(xrange(1, netdata.get_total_num_nodes()+1), max_tries)
    num_friends_matched = 0
    r_index = 0
    r_index2 = 0
    tries = 0
    avg_fsim = 0
    avg_rsim = 0
    #found_ids = {} #TODO think of ways to avoid duplicates and ensure every non-friend is tried once, also how to free memory
    control_nonfr_nodes = <netnodedata **>malloc(num_eligible_friends*sizeof(netnodedata *))
    selected_friends = <netnodedata **>malloc(num_eligible_friends*sizeof(netnodedata *))

    rand_node_ids = <int *>malloc(max_tries*sizeof(int))
    for j in range(max_tries): 
        rand_node_ids[j] = (rand() % (max_tries-1)) + 1 # TODO does not cover all non-friends
    
    for r_index in range(max_tries):#, schedule='dynamic', num_threads=1):
        rand_node = (<netnodedata *>allnodes[rand_node_ids[r_index]])[0]
        frnonfr_data =  compare_friend_nonfriend(c_node, rand_node, allnodes,
            num_eligible_friends, frsim_data_arr,interact_type, 
            min_interactions_per_user, data_type_code,
            time_diff, time_scale, max_sim_ratio_error, max_interact_ratio_error)
        if frnonfr_data.fsim > -0.5: # workaround for float equality
                if num_friends_matched < num_eligible_friends:
                    avg_fsim += frnonfr_data.fsim
                    avg_rsim += frnonfr_data.rsim
                    control_nonfr_nodes[num_friends_matched] = &(frnonfr_data.rnode)
                    selected_friends[num_friends_matched] = &(frnonfr_data.fnode)
                    num_friends_matched += 1
                else:
                    break
                    #printf("We got a matching!! %f %f", fsim, rsim)

    tries += 1
    #printf("Eligible friends and matched: %d %d\n`", num_eligible_friends, num_friends_matched) 
    if num_eligible_friends >0 and num_friends_matched >= min_friends_match_ratio*num_eligible_friends:
        avg_fsim = avg_fsim/float(num_friends_matched)
        avg_rsim = avg_rsim/float(num_friends_matched)
    #TODO even this struct c_node should be referenced, not copy for fn call 
        inf1 = compute_node_susceptibility(c_node,selected_friends, num_friends_matched, interact_type,
                cutoff_rating, data_type_code2, min_interactions_per_user, 
                time_diff, time_scale, allow_duplicates, False)
        #print inf1
        if inf1 > -1: #TODO check for none, remove default argument debug
            ptr_node_fnode_counter[0] += 1
            inf2 = compute_node_susceptibility(c_node,control_nonfr_nodes, num_friends_matched, 
                    interact_type, cutoff_rating, data_type_code2, 
                    min_interactions_per_user, 
                    time_diff, time_scale, allow_duplicates, False)
        #print node.uid, inf1, inf2, "\n"
            if inf2 > -1:
                ptr_node_rnode_counter[0] += 1
                node_data.fsim = avg_fsim
                node_data.rsim = avg_rsim
                node_data.fsusc = inf1
                node_data.rsusc = inf2
    #printf("Good node %d",node_rnode_counter) 
    free(control_nonfr_nodes)
    free(selected_friends)
    free(frsim_data_arr)
    return node_data


cdef compfrnonfr compare_friend_nonfriend(netnodedata c_node, netnodedata rand_node,
        netnodedata **allnodes,
        int num_eligible_friends, fsiminfo *frsim_data_arr,int interact_type, 
        int min_interactions_per_user, int data_type_code,
        int time_diff, int time_scale, float max_sim_ratio_error, float max_interact_ratio_error) nogil:
    cdef:
        float rsim, fsim, sim_diff
        float ratio_train
        int f_index
        netnodedata fobj
        compfrnonfr ret
    ret.fsim = -1
    if rand_node.c_length_train_ids >=min_interactions_per_user and rand_node.c_length_test_ids >=min_interactions_per_user:
        if True:
            #if rand_node.uid not in friend_ids and 
            if rand_node.c_uid!=c_node.c_uid:
                rsim = compute_node_similarity(c_node,rand_node, interact_type, 
                                                data_type_code, min_interactions_per_user, 
                                                time_diff=-1, time_scale=time_scale)
                if rsim > -0.5: # also check for duplicate friend being selected
                    f_index = binary_search_closest_fsiminfo(frsim_data_arr, num_eligible_friends, rsim, 0)
                    if f_index == num_eligible_friends:
                        f_index = f_index - 1
                    fsim = frsim_data_arr[f_index].sim
                    fobj = (<netnodedata *>allnodes[frsim_data_arr[f_index].uid])[0]
                    sim_diff = fabs(rsim-fsim)
                    ratio_train = abs(rand_node.c_length_train_ids-fobj.c_length_train_ids)/<float>(fobj.c_length_train_ids)
                    if ratio_train <= max_interact_ratio_error: 
        
                        if (fsim< 0.000001 and rsim<0.000001) or (fsim>0 and
                            sim_diff/fsim <= max_sim_ratio_error):
                                ret.fsim = fsim
                                ret.rsim = rsim
                                ret.fnode = fobj
                                ret.rnode = rand_node
    return ret

cdef float compute_node_susceptibility(netnodedata my_node, netnodedata **other_nodes,
        int length_other_nodes,
        int interact_type, float cutoff_rating, int data_type_code, 
        int min_interactions_per_user, int time_diff, int time_scale, 
        bint allow_duplicates, bint debug) nogil:
    cdef int length_my_interactions
    cdef int *lengths_others_interactions = <int *>malloc(length_other_nodes*cython.sizeof(int))
    cdef idata *my_interactions
    cdef idata **others_interactions = <idata **>malloc(length_other_nodes*sizeof(idata *))
    cdef int i
    cdef float ret = -1
    
    if data_type_code == <int>'i':
        my_interactions = my_node.c_test_data
        length_my_interactions = my_node.c_length_test_ids
        valid_flag = True
        for i in range(length_other_nodes):
            c_node_obj = other_nodes[i][0]
            lengths_others_interactions[i] = c_node_obj.c_length_test_ids
            others_interactions[i] = c_node_obj.c_test_data
            if lengths_others_interactions[i] < min_interactions_per_user:
                valid_flag = False
                break
        if length_my_interactions >= min_interactions_per_user and valid_flag:
            if time_scale == <int>'o':
                ret = compute_node_susceptibility_ordinal_c(my_interactions, 
                        length_my_interactions, others_interactions, 
                        lengths_others_interactions, length_other_nodes, interact_type, 
                        data_type_code,
                        time_diff=time_diff, cutoff_rating=cutoff_rating, 
                        time_scale=time_scale, allow_duplicates=allow_duplicates, debug=debug)
            else:
                ret = compute_node_susceptibility_c(my_interactions, 
                        length_my_interactions, others_interactions, 
                        lengths_others_interactions, length_other_nodes, interact_type, 
                        data_type_code,
                        time_diff=time_diff, cutoff_rating=cutoff_rating, time_scale=time_scale)
    else:
        printf("Error: data_type_code invalid for computing susceptibility")
    free(lengths_others_interactions)
    free(others_interactions)
    return ret


cdef float compute_node_similarity(netnodedata node, netnodedata other_node, int interact_type, 
        int data_type_code, int min_interactions_per_user, 
        int time_diff, int time_scale) nogil:
    cdef int length_my_interactions
    cdef int length_other_interactions
    cdef idata *my_interactions
    cdef idata *other_interactions
    cdef netnodedata c_node1, c_node2
    c_node2 = other_node
    c_node1 = node
    cdef float sim = -1
    if data_type_code == <int>'a':
        my_interactions = c_node1.c_list[interact_type]
        length_my_interactions = c_node1.c_length_list[interact_type]
        other_interactions = c_node2.c_list[interact_type]
        length_other_interactions = c_node2.c_length_list[interact_type]
    elif data_type_code == <int>'c':
        my_interactions = c_node1.c_train_data
        length_my_interactions = c_node1.c_length_train_ids
        other_interactions = c_node2.c_train_data
        length_other_interactions = c_node2.c_length_train_ids
        
    elif data_type_code == <int>'i':
        my_interactions = c_node1.c_test_data
        length_my_interactions = c_node1.c_length_test_ids
        other_interactions = c_node2.c_test_data
        length_other_interactions = c_node2.c_length_test_ids
    #with nogil: 
    if length_my_interactions >= min_interactions_per_user and (
            length_other_interactions >= min_interactions_per_user):
        sim = compute_node_similarity_c(my_interactions, other_interactions,
                length_my_interactions, length_other_interactions, 
                time_diff=time_diff,
                time_scale=time_scale)
    return sim

# CAUTION: works only for wall-clock time_scale right now
cdef float compute_node_similarity_c(idata *my_interactions, idata *others_interactions, 
                                     int length_my_interactions,
                                     int length_others_interactions, 
                                     int time_diff, 
                                     int time_scale) nogil:
    cdef float l2_norm1, l2_norm2, simscore, union_sum

    cdef int i, j, k
    simscore = 0
    i = 0 
    j = 0
    cdef int my_count = 0
    cdef int others_count = 0
    while i < length_my_interactions and j < length_others_interactions:
        if my_interactions[i].item_id < others_interactions[j].item_id:
            my_count += 1
            i += 1
        elif my_interactions[i].item_id > others_interactions[j].item_id:
            others_count += 1
            j+= 1
        else: # item ids are equal
            if time_diff == -1 or abs(my_interactions[i].timestamp - others_interactions[j].timestamp) <= time_diff:
                simscore += 1
            """
            if is_influence(my_interactions, length_my_interactions,
                    my_interactions[i].timestamp, 
                    others_interactions[j].timestamp, -1, #cutoffrating,toremove
                    time_diff, time_scale, order_agnostic=True):
                simscore +=1
            """

            my_count += 1
            others_count += 1
            i += 1
            j += 1
    if j == length_others_interactions:
        for k in range(i,length_my_interactions):
            my_count += 1
    elif i == length_my_interactions:
        for k in range(j, length_others_interactions):
            others_count += 1

    if my_count == 0 or others_count == 0:
        return -1
    union_sum = my_count + others_count - simscore

    return simscore/union_sum

cdef int get_num_interactions_between(idata *interactions, int length_interactions,
        int timestamp1, 
        int timestamp2, float cutoff_rating, bint order_agnostic=True) nogil:
    cdef int i 
    cdef int num_interacts_between = 0
    for i in range(length_interactions):
        if interactions[i].rating >= cutoff_rating:
            if interactions[i].timestamp >timestamp1 and interactions[i].timestamp <=timestamp2:
                num_interacts_between += 1
            elif order_agnostic:
                if interactions[i].timestamp >=timestamp2 and interactions[i].timestamp <timestamp1:
                    num_interacts_between += 1
    return num_interacts_between

cdef bint is_influence(idata *my_interactions, int length_my_interactions, 
        int my_timestamp, int others_timestamp, float cutoff_rating,
        int time_diff, int time_scale, bint order_agnostic) nogil:
    cdef bint inf = 0
    
    if time_diff == -1:
        inf = 1
    elif time_scale==<int>'w':
        if order_agnostic:
            if abs(my_timestamp - others_timestamp) <= time_diff:
                inf = 1
        else:
            if my_timestamp > others_timestamp and (my_timestamp - others_timestamp) <= time_diff:
                inf = 1
    elif time_scale==<int>'o':
        if get_num_interactions_between(my_interactions, length_my_interactions,
                others_timestamp, my_timestamp, 
                cutoff_rating, order_agnostic=order_agnostic) <=time_diff:
            inf = 1

    return inf



cpdef compute_global_topk_similarity(all_nodes, interact_type, klim):
    cdef CNetworkNode c_node_obj
    cdef int total_interactions = 0
    cdef int total_nodes = 0
    for node_obj in all_nodes:
        c_node_obj = <CNetworkNode>node_obj
        total_interactions += c_node_obj.ndata.c_length_list[interact_type]
        total_nodes += 1

    cdef np.ndarray[DTYPE_t, ndim=1] data = np.empty(total_interactions, dtype=DTYPE)
    cdef np.ndarray[DTYPE_INT_t, ndim=1] indices = np.empty(total_interactions, dtype=DTYPE_INT)
    cdef np.ndarray[DTYPE_INT_t, ndim=1] indptr = np.empty(total_nodes+1+1, dtype=DTYPE_INT)
    cdef int curr_index = 0
    cdef DTYPE_t l2_norm
    cdef int i,j
    i = 0
    indptr[0:2]=0
    i+=1
    for node_obj in all_nodes:
        c_node_obj = <CNetworkNode>node_obj
        i+=1
        l2_norm = sqrt(c_node_obj.ndata.c_length_list[interact_type])
        for j in range(c_node_obj.ndata.c_length_list[interact_type]):
            data[curr_index] = 1/l2_norm
            indices[curr_index] = c_node_obj.ndata.c_list[interact_type][j].item_id
            curr_index += 1
        indptr[i] = indptr[i-1] + c_node_obj.ndata.c_length_list[interact_type]

    mat = sp.csr_matrix((data, indices, indptr))
    #cc = mat * mat.transpose()
    #print mat[6,346165], mat[1,1], mat[1,1669118]
    #print data.shape[0]
    #print mat.shape[0], mat.shape[1]
    #print cc.shape[0], cc.shape[1]
    return mat

"""
cdef compute_node_similarity_c(int* my_interactions, int *other_interactions, int length_my_interactions, int length_other_interactions):
    cdef float l2_norm1, l2_norm2, simscore
    cdef int i, j
    simscore = 0
    i = 0
    j = 0
    while i < length_my_interactions and j < length_other_interactions:
        if my_interactions[i] < other_interactions[j]:
            i += 1
        elif my_interactions[i] > other_interactions[j]:
            j+= 1
        else:
            simscore +=1
            i += 1
            j += 1
    l2_norm1 = sqrt(length_my_interactions)
    l2_norm2 = sqrt(length_other_interactions)

    return simscore/(l2_norm1*l2_norm2)
"""

cdef idata *create_nodes_interaction_stream(idata **nodes_interactions, 
        int *lengths_nodes_interactions, int num_nodes, int *size_stream) nogil:
    cdef int total_interactions = 0
    cdef int i,  curr_ptr
    cdef idata *interaction_stream = NULL
    for i in range(num_nodes):
        total_interactions += lengths_nodes_interactions[i]
    interaction_stream = <idata *>malloc(total_interactions*cython.sizeof(idata))
    if interaction_stream is NULL:
        #raise MemoryError()
        printf("ERROR:Cannot allocate memory for interaction stream!!!\n\n")
   
    curr_ptr = 0
    #print num_nodes, "num nodes"
    for i in range(num_nodes):
        memcpy(&(interaction_stream[curr_ptr]), nodes_interactions[i], lengths_nodes_interactions[i]*cython.sizeof(idata))
        curr_ptr += lengths_nodes_interactions[i]
    qsort(interaction_stream, total_interactions, cython.sizeof(idata), comp_interactions_temporal)
    size_stream[0] = total_interactions
    return interaction_stream

# CAUTIONL only works for ordinal scale time
cdef float compute_node_susceptibility_ordinal_c(idata* my_interactions, 
                        int length_my_interactions, idata** others_interactions, 
                        int* lengths_others_interactions, int num_other_nodes, 
                        int interact_type, int data_type_code, 
                        int time_diff, float cutoff_rating,
                        int time_scale, bint allow_duplicates, bint debug) nogil:
    cdef float simscore= 0
    cdef int i, j, y, index
    cdef int size_stream = -1
    cdef int prev_item_id = -1
    cdef idata *others_stream = create_nodes_interaction_stream(others_interactions,
            lengths_others_interactions, num_other_nodes, &size_stream)
    cdef int my_count = 0
    cdef int others_count = 0
    cdef int *sim = <int *>malloc(length_my_interactions*cython.sizeof(int))
    for y in range(length_my_interactions):
        sim[y] = 0
    i = 0
    while i < length_my_interactions:
        if my_interactions[i].rating >= cutoff_rating:
            # Checking for duplicates, we know my_interactions is sorted by item_id
            if allow_duplicates or my_interactions[i].item_id !=prev_item_id:
                index = binary_search_closest_temporal(others_stream, size_stream, 
                        my_interactions[i].timestamp, debug)
                if index >= size_stream:
                    if debug:
                        printf("ERROR:Cython: Big Error, how can index of friend stream be %d", index)
                if index <0:
                    if debug:
                        printf("WARN:This interaction is too early for others test set. Aborting...")
                    return -2 
                if index+1 <time_diff:
                    if debug:
                        printf("WARN:Oh, cannot find enough nodes to compare. Too early for others test set. Aborting...")
                    return -3
                j=index
                while j >index-time_diff and j>=0:
                    #print others_stream[j].item_id,  my_interactions[i].item_id, others_stream[j].timestamp,my_interactions[i].timestamp
                    if others_stream[j].item_id == my_interactions[i].item_id:
                        if others_stream[j].rating >= cutoff_rating:
                            sim[i] += 1
                    j -= 1
                """
                if sim[i] == 0:
                    print "ONO!!!", my_interactions[i].item_id, my_interactions[i].timestamp
                """
                my_count += 1
        prev_item_id = my_interactions[i].item_id
        i += 1


    if my_count == 0:
        return -1
    #l2_norm1 = sqrt(my_count)
    for y in range(length_my_interactions):
        if sim[y] > 0:
            simscore += 1
    free(others_stream)
    free(sim)
    return simscore/my_count

cdef float compute_node_susceptibility_c(idata* my_interactions, 
                        int length_my_interactions, idata** others_interactions, 
                        int* lengths_others_interactions, int num_other_nodes, 
                        int interact_type, int data_type_code, 
                        int time_diff, float cutoff_rating,
                        int time_scale) nogil:
    cdef float l2_norm1, l2_norm2, simscore

    cdef int i, k, x, y
    cdef int *j = <int *>malloc(num_other_nodes*cython.sizeof(int))
    cdef int *sim = <int *>malloc(length_my_interactions*cython.sizeof(int))
    for x in range(num_other_nodes):
        j[x] = 0
    for y in range(length_my_interactions):
        sim[y] = 0
    simscore = 0
    i = 0 
    cdef int my_count = 0
    cdef int others_count = 0
    cdef bint finished_others = False
    while i < length_my_interactions:
        for x in xrange(num_other_nodes): 
#if my_interactions[i].item_id < others_interactions[x][j[x]].item_id:
#                i += 1
            while j[x] < lengths_others_interactions[x] and my_interactions[i].item_id > others_interactions[x][j[x]].item_id: 
#if others_interactions[x][j[x]].rating >= cutoff_rating:
#                    others_count += 1
                j[x] += 1
            if j[x] < lengths_others_interactions[x] and my_interactions[i].item_id == others_interactions[x][j[x]].item_id:
                if my_interactions[i].rating >= cutoff_rating and others_interactions[x][j[x]].rating >= cutoff_rating:
                    if is_influence(my_interactions, length_my_interactions,
                            my_interactions[i].timestamp, 
                            others_interactions[x][j[x]].timestamp, cutoff_rating,
                            time_diff, time_scale, order_agnostic=False):
                        sim[i] += 1
                #if others_interactions[x].rating >= cutoff_rating:
                #    others_count += 1
                j[x] += 1
        i +=1
        if my_interactions[i].rating >= cutoff_rating:
            my_count += 1


    if my_count == 0:
        return -1
    #l2_norm1 = sqrt(my_count)
    for y in range(length_my_interactions):
        if sim[y] > 0:
            simscore += 1
    free(j)
    free(sim)
    return simscore/my_count



cdef float l2_norm_dict(dict_val_iter):
    cdef float norm = 0
    cdef float temp
    for v in dict_val_iter:
        temp = len(v)
        norm += temp*temp
    norm = sqrt(norm)
    return norm



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
    cdef netnodedata ndata
    cdef int c_should_have_friends
    cdef int c_should_have_interactions
    cdef int c_num_interact_types   # number of interactions types

   
    # Additional data structures for simulation. Consider removing.
    cdef idata *fakedata_inf_fr_stream
    cdef int length_fakedata_inf_fr_stream

    def __cinit__(self, *args,  **kwargs):
        cdef int a
        if kwargs['should_have_interactions'] and not (kwargs['node_data'] is None):
            self.c_num_interact_types = len(kwargs['node_data'].interaction_types)
            self.ndata.c_list = <idata **>PyMem_Malloc(self.c_num_interact_types*sizeof(idata *))
            self.ndata.c_length_list = <int *>PyMem_Malloc(self.c_num_interact_types*cython.sizeof(int))
            for a in xrange(self.c_num_interact_types):
                self.ndata.c_length_list[a] = 0
        else:
            self.c_num_interact_types = 0
            self.ndata.c_list = NULL
            self.ndata.c_length_list = NULL

        self.ndata.c_friend_list = NULL
        self.ndata.c_train_data = NULL
        self.ndata.c_test_data = NULL
        self.ndata.c_length_friend_list = 0
        self.ndata.c_length_test_ids = 0
        self.ndata.c_length_train_ids = 0
        #if kwargs['should_have_friends']:
        #    self.ndata.c_friend_list = <fdata **>PyMem_Malloc(self.)a
        self.length_fakedata_inf_fr_stream = -1
    
    def __init__(self,*args, **kwargs):
        self.ndata.c_uid = int(args[0])
        self.c_should_have_friends = kwargs['should_have_friends']
        self.c_should_have_interactions = kwargs['should_have_interactions']

    def __dealloc__(self):
        cdef int interact_type
        if self.ndata.c_list is not NULL:
            for interact_type in xrange(self.c_num_interact_types):
                if self.ndata.c_length_list[interact_type] > 0:
                    PyMem_Free(self.ndata.c_list[interact_type])
            PyMem_Free(self.ndata.c_list)
            PyMem_Free(self.ndata.c_length_list)
        
        if self.ndata.c_friend_list is not NULL:
            PyMem_Free(self.ndata.c_friend_list)
        
        if self.ndata.c_train_data is not NULL:
            PyMem_Free(self.ndata.c_train_data)
            PyMem_Free(self.ndata.c_test_data)
    
    cpdef int store_interactions(self, int interact_type, ilist, do_sort=True) except -1:
        self.ndata.c_list[interact_type] = <idata *>PyMem_Malloc(len(ilist)*cython.sizeof(idata))
        if self.ndata.c_list is NULL:
            raise MemoryError()
        cdef int i
        for i in range(len(ilist)):
            self.ndata.c_list[interact_type][i].item_id = ilist[i].item_id
            self.ndata.c_list[interact_type][i].rating = ilist[i].rating
            self.ndata.c_list[interact_type][i].timestamp = ilist[i].timestamp
        self.ndata.c_length_list[interact_type] = len(ilist)
        if do_sort:
            qsort(self.ndata.c_list[interact_type], self.ndata.c_length_list[interact_type], sizeof(idata), comp_interactions)
        return self.ndata.c_length_list[interact_type]

    cpdef int store_friends(self, flist):
        self.ndata.c_friend_list = <fdata *>PyMem_Malloc(len(flist)*cython.sizeof(fdata))
        if self.ndata.c_friend_list is NULL:
            raise MemoryError()
        cdef int i
        for i in xrange(len(flist)):
            self.ndata.c_friend_list[i].receiver_id = flist[i].receiver_id
        self.ndata.c_length_friend_list = len(flist)
        
        qsort(self.ndata.c_friend_list, self.ndata.c_length_friend_list, sizeof(fdata), comp_friends)
        return self.ndata.c_length_friend_list
    
    cpdef int store_friend_ids(self, flist):
        self.ndata.c_friend_list = <fdata *>PyMem_Malloc(len(flist)*cython.sizeof(fdata))
        if self.ndata.c_friend_list is NULL:
            raise MemoryError()
        cdef int i
        for i in xrange(len(flist)):
            self.ndata.c_friend_list[i].receiver_id = flist[i]
        self.ndata.c_length_friend_list = len(flist)
        return self.ndata.c_length_friend_list
    
    cpdef int remove_interactions(self, interact_type):
        PyMem_Free(self.ndata.c_list[interact_type])
        self.ndata.c_length_list[interact_type] = 0
        return 1

    cpdef int remove_friends(self, change_should_have_friends=True):
        self.ndata.c_length_friend_list = 0
        PyMem_Free(self.ndata.c_friend_list)
        self.ndata.c_friend_list = NULL
        if change_should_have_friends:
            self.c_should_have_friends = False

    cpdef get_all_items_interacted_with(self):
        interacted_items = set()
        cdef int i, curr_interact_type
        for curr_interact_type in range(self.c_num_interact_types):
            for i in range(self.ndata.c_length_list[curr_interact_type]):
                #print self.ndata.c_list[curr_interact_type][i].item_id           
                interacted_items.add(self.ndata.c_list[curr_interact_type][i].item_id)
        return interacted_items

    def print_interactions(self, interact_type):
        print self.ndata.c_uid
        for i in range(self.ndata.c_length_list[interact_type]):
            print "---", self.ndata.c_list[interact_type][i].item_id

    cpdef get_items_interacted_with(self, int interact_type, 
            int rating_cutoff=-1, bool return_timestamp = False):
        interacted_items = set()
        cdef int i
        for i in range(self.ndata.c_length_list[interact_type]):
            if self.ndata.c_list[interact_type][i].rating >= rating_cutoff:
                if return_timestamp:
                    interacted_items.add((self.ndata.c_list[interact_type][i].item_id, self.c_list[interact_type][i].timestamp))
                else:
                    interacted_items.add(self.ndata.c_list[interact_type][i].item_id)
        #print self.ndata.c_uid, len(interacted_items), interact_type
        return interacted_items
   
        
    cpdef get_interactions(self, int interact_type, int cutoff_rating=-1):
        interacts = []
        cdef int i
        for i in range(self.ndata.c_length_list[interact_type]):
            if self.ndata.c_list[interact_type][i].rating >= cutoff_rating:
                interacts.append((self.ndata.c_list[interact_type][i].item_id, 
                            self.ndata.c_list[interact_type][i].timestamp,
                            self.ndata.c_list[interact_type][i].rating))
        return interacts

    cdef idata* get_interactions_c(self, int interact_type, int *length_list, int data_type):
        if data_type == <int>"a":
            length_list[0] = self.ndata.c_length_list[interact_type]
            return self.ndata.c_list[interact_type]
        else:
            print "woow"

    cpdef get_interactions_before(self, int interact_type, int split_timestamp,
            int cutoff_rating=-1):
        interacts = []
        cdef int i
        for i in range(self.ndata.c_length_list[interact_type]):
            if self.ndata.c_list[interact_type][i].rating >= cutoff_rating and (
                    self.ndata.c_list[interact_type][i].timestamp < split_timestamp):
                interacts.append((self.ndata.c_list[interact_type][i].item_id, 
                            self.ndata.c_list[interact_type][i].timestamp,
                            self.ndata.c_list[interact_type][i].rating))
        return interacts

    cpdef get_interactions_after(self, int interact_type, int split_timestamp,
            int cutoff_rating=-1):
        interacts = []
        cdef int i
        for i in range(self.ndata.c_length_list[interact_type]):
            if self.ndata.c_list[interact_type][i].rating >= cutoff_rating and (
                    self.ndata.c_list[interact_type][i].timestamp >= split_timestamp):
                interacts.append((self.ndata.c_list[interact_type][i].item_id, 
                            self.ndata.c_list[interact_type][i].timestamp,
                            self.ndata.c_list[interact_type][i].rating))
        return interacts
    
    cpdef get_interactions_between(self, int interact_type, int start_timestamp,
            int end_timestamp,
            int cutoff_rating=-1):
        interacts = []
        cdef int i
        for i in range(self.ndata.c_length_list[interact_type]):
            if self.ndata.c_list[interact_type][i].rating >= cutoff_rating and (
                    self.ndata.c_list[interact_type][i].timestamp < end_timestamp) and (
                        self.ndata.c_list[interact_type][i].timestamp >= start_timestamp):
                interacts.append((self.ndata.c_list[interact_type][i].item_id, 
                            self.ndata.c_list[interact_type][i].timestamp,
                            self.ndata.c_list[interact_type][i].rating))
        return interacts

    cpdef get_friend_ids(self):
        cdef int i
        id_list = []
        for i in xrange(self.ndata.c_length_friend_list):
            id_list.append(self.ndata.c_friend_list[i].receiver_id)
        return id_list
    
    
    cpdef get_num_interactions(self, int interact_type):
        if self.ndata.c_list is NULL:
            return 0
        else:
            return self.ndata.c_length_list[interact_type]

    cpdef has_interactions(self, int interact_type):
        return (not (self.ndata.c_list is NULL)) and (self.ndata.c_length_list[interact_type] > 0)

    cpdef get_num_friends(self):
        if self.ndata.c_friend_list is NULL:
            return 0
        else:
            return self.ndata.c_length_friend_list

    cpdef has_friends(self):
        return (not(self.ndata.c_friend_list is NULL)) and (self.ndata.c_length_friend_list > 0)
    
    cpdef change_interacted_item(self, interact_type, old_item_id, new_item_id, timestamp):
        cdef int old_index = binary_search_timesensitive(self.ndata.c_list[interact_type], 
                self.ndata.c_length_list[interact_type], old_item_id, timestamp, False)
        cdef idata temp_inter
        if old_index == -1:
            for i in range(self.ndata.c_length_list[interact_type]):
                if self.ndata.c_list[interact_type][i].item_id == old_item_id and (
                        self.ndata.c_list[interact_type][i].timestamp == timestamp):
                    print i, old_item_id, timestamp
            return binary_search_timesensitive(self.ndata.c_list[interact_type],
                    self.ndata.c_length_list[interact_type], old_item_id, timestamp, True)
        else:
            new_index = binary_search_closest(self.ndata.c_list[interact_type], 
                    self.ndata.c_length_list[interact_type], new_item_id, False)
            if new_index == -1:
                print "new_index is not correct"
                return -1
            temp_inter = self.ndata.c_list[interact_type][old_index] 
            temp_inter.item_id = new_item_id

            if old_index >= new_index:
                memmove(&self.ndata.c_list[interact_type][new_index+1],
                        &self.ndata.c_list[interact_type][new_index], 
                        (old_index-new_index)*cython.sizeof(idata))
                self.ndata.c_list[interact_type][new_index] = temp_inter
            elif old_index < new_index:
                memmove(&self.ndata.c_list[interact_type][old_index], 
                        &self.ndata.c_list[interact_type][old_index+1], 
                        (new_index-old_index-1)*cython.sizeof(idata))
                self.ndata.c_list[interact_type][new_index -1] = temp_inter
                
            #self.ndata.c_list[interact_type][index].item_id = new_item_id
            return 1

    cpdef compute_similarity(self, items, int interact_type, int rating_cutoff):
        if self.ndata.c_length_list[interact_type] ==0 or len(items) == 0:
            return None 
        cdef float simscore = 0
        cdef float l2_norm1, l2_norm2
        cdef int i, item_id
        cdef int count_items = 0
        for i in range(self.ndata.c_length_list[interact_type]):
            if self.ndata.c_list[interact_type][i].rating > rating_cutoff:
                item_id = self.ndata.c_list[interact_type][i].item_id
                if item_id in items:
                    simscore += len(items[item_id])
                count_items += 1
        if count_items == 0:
            return None
        #l2_norm1 = sqrt(self.ndata.c_length_list[interact_type])
        l2_norm1 = sqrt(count_items)
        l2_norm2 = l2_norm_dict(items.itervalues())
        return simscore/(l2_norm1*l2_norm2)



        """
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
        """


    
    cdef float compute_othernode_influence_c(self, idata *others_interactions, 
                                         int length_others_interactions, 
                                         int interact_type, int time_diff,
                                         float cutoff_rating, int data_type_code,
                                         int time_scale):
        cdef float l2_norm1, l2_norm2, simscore
        cdef idata *my_interactions = NULL
        cdef int length_my_interactions = 0
        if data_type_code==<int>'i':
            my_interactions = self.ndata.c_test_data
            length_my_interactions = self.ndata.c_length_test_ids
        cdef int i, j, k
        simscore = 0
        i = 0 
        j = 0       
        # Variables to check whether my_interactions or others_interactions are empty
        cdef int my_count = 0
        cdef int others_count = 0
        while i < length_my_interactions and j < length_others_interactions:
            if my_interactions[i].item_id < others_interactions[j].item_id:
                if my_interactions[i].rating >= cutoff_rating:
                    my_count += 1
                i += 1
            elif my_interactions[i].item_id > others_interactions[j].item_id:
                if others_interactions[j].rating >= cutoff_rating:
                    others_count += 1
                j+= 1
            else:
                if my_interactions[i].rating >= cutoff_rating and others_interactions[j].rating >= cutoff_rating:
                    if is_influence(my_interactions, length_my_interactions,
                            my_interactions[i].timestamp, 
                            others_interactions[j].timestamp, cutoff_rating, 
                            time_diff, time_scale, order_agnostic=False):
                        simscore += 1

                if my_interactions[i].rating >= cutoff_rating:
                    my_count += 1
                if others_interactions[j].rating >= cutoff_rating:
                    others_count += 1
                i += 1
                j += 1

        if j == length_others_interactions:
            for k in range(i,length_my_interactions):
                if my_interactions[k].rating >= cutoff_rating:
                    my_count += 1
        elif i == length_my_interactions:
            for k in range(j, length_others_interactions):
                if others_interactions[k].rating >= cutoff_rating:
                    others_count += 1

        if my_count == 0 or others_count == 0: # prevent zero division error
            return -1
        l2_norm1 = my_count
        l2_norm2 = others_count

        return simscore/(l2_norm2)

    @cython.boundscheck(False)
    cpdef compute_global_topk_similarity(self, allnodes_iterable, int interact_type, int klim, float cutoff_rating):
        cdef np.ndarray[DTYPE_t, ndim=1] sims_vector = np.zeros(klim, dtype=DTYPE)

        cdef fdata *friend_arr = self.ndata.c_friend_list
        cdef int i, min_sim_index
        cdef DTYPE_t sim, min_sim
        cdef CNetworkNode c_node_obj
        min_sim = 0
        min_sim_index = 0
        for node_obj in allnodes_iterable:
            c_node_obj = <CNetworkNode>node_obj
            if c_node_obj.ndata.c_length_list[interact_type]>0:
                if (not exists(c_node_obj.ndata.c_uid, friend_arr, self.ndata.c_length_friend_list)) and c_node_obj.ndata.c_uid != self.ndata.c_uid:
                    sim = compute_node_similarity_c(self.ndata.c_list[interact_type], c_node_obj.ndata.c_list[interact_type], self.ndata.c_length_list[interact_type], c_node_obj.ndata.c_length_list[interact_type], -1, time_scale=<int>'w')
                    #sim=1
                    if sim > min_sim:
                        sims_vector[min_sim_index] = sim
                        min_sim = min(sims_vector, klim, &min_sim_index)
        return sims_vector

    @cython.boundscheck(False)
    cpdef compute_global_topk_similarity_mat(self, mat, int klim):
        #print mat.shape[0], mat.shape[1]
        #print self.uid
        user_row = mat[self.uid,:]
        sim_col = mat * user_row.T
        #sim_indices,_=sim_col.nonzero()
        cdef np.ndarray[DTYPE_t, ndim=1] sims_vector = np.zeros(klim, dtype=DTYPE)

        cdef int min_sim_index = 0
        cdef DTYPE_t min_sim = 0
        cdef DTYPE_t val
        min_sim_index = 0
        cdef int i
        for i in range(sim_col.shape[0]):
            val = sim_col[i,0]
            #print val
            if val > min_sim:
                sims_vector[min_sim_index] = val
                min_sim = min(sims_vector, klim, &min_sim_index)

        return sims_vector

    @cython.boundscheck(False)
    cpdef compute_local_topk_similarity(self, friends_iterable, int interact_type, int klim, float cutoff_rating):
        cdef np.ndarray[DTYPE_t, ndim=1] sims_vector = np.zeros(klim, dtype=DTYPE)
        #cdef fdata *friend_arr = self.ndata.c_friend_list
        cdef int i, min_sim_index
        cdef DTYPE_t sim, min_sim
        cdef CNetworkNode c_node_obj
        min_sim = 0
        min_sim_index = 0
        cdef int counter = 0
        for node_obj in friends_iterable:
            c_node_obj = <CNetworkNode>node_obj
            if self.ndata.c_length_list[interact_type]>0 and c_node_obj.ndata.c_length_list[interact_type]>0: 
                sim = compute_node_similarity_c(self.ndata.c_list[interact_type], c_node_obj.ndata.c_list[interact_type], self.ndata.c_length_list[interact_type], c_node_obj.ndata.c_length_list[interact_type], -1, time_scale=<int>'w')
                if sim > min_sim:
                    sims_vector[min_sim_index] = sim
                    min_sim = min(sims_vector, klim, &min_sim_index)
                counter += 1
        if counter < klim:
            sims_vector = None
        return sims_vector

    @cython.boundscheck(False)
    cpdef  compute_mean_similarity(self, nodes_iterable, int interact_type, float cutoff_rating):
        cdef int i, min_sim_index
        cdef DTYPE_t sim
        cdef DTYPE_t sim_avg = 0
        cdef int counter = 0
        cdef CNetworkNode c_node_obj
        for node_obj in nodes_iterable:
            c_node_obj = <CNetworkNode>node_obj
            if self.ndata.c_length_list[interact_type]>0 and c_node_obj.ndata.c_length_list[interact_type]>0:
                sim = compute_node_similarity_c(self.ndata.c_list[interact_type], c_node_obj.ndata.c_list[interact_type],self.ndata.c_length_list[interact_type], c_node_obj.ndata.c_length_list[interact_type], -1, time_scale=<int>'w')
                if sim != -1: #TODO CAUTION change float eq comparison
                    sim_avg += sim
                    counter += 1
        if counter == 0:
            return None
        else:
            return sim_avg/counter

    cpdef compute_items_coverage(self, friends_iterable, int interact_type, int min_friends, int max_friends):
        if self.ndata.c_length_list[interact_type] == 0 or self.ndata.c_length_friend_list < min_friends or self.ndata.c_length_friend_list >= max_friends:
            return None
        cdef int_counter *all_items = NULL
        cdef int_counter *s = NULL
        cdef int i
        cdef int num_items_found = 0
        for node_obj in friends_iterable:
            c_node_obj = <CNetworkNode>node_obj
            for i in range(c_node_obj.ndata.c_length_list[interact_type]):
                HASH_FIND_INT(all_items, &(c_node_obj.ndata.c_list[interact_type][i].item_id), s)
                if s == NULL:
                    s = <int_counter *>PyMem_Malloc(cython.sizeof(int_counter))
                    s[0].id = c_node_obj.ndata.c_list[interact_type][i].item_id
                    s[0].count = 0
                    HASH_ADD_INT_CUSTOM(all_items, s)
                s[0].count += 1

        s = NULL
        for i in range(self.ndata.c_length_list[interact_type]):
            HASH_FIND_INT(all_items, &(self.ndata.c_list[interact_type][i].item_id), s)
            if s != NULL:
                num_items_found += 1

        delete_hashtable(all_items)
        return (<float>num_items_found)/self.ndata.c_length_list[interact_type]

    cdef int_counter *update_items_edge_coverage_c(self, idata* others_interactions, int length_others_interactions, int interact_type, int_counter *count_influencers, bool time_sensitive, int time_diff):
        cdef idata *my_interactions = self.ndata.c_list[interact_type]
        cdef int i, j, valid_match
        i = 0
        j = 0
        #cdef int my_count = 0
        #cdef int others_count = 0
        while i < self.ndata.c_length_list[interact_type] and j < length_others_interactions:
            if my_interactions[i].item_id < others_interactions[j].item_id:
                #if my_interactions[i].rating > cutoff_rating:
                #    my_count += 1
                i += 1
            elif my_interactions[i].item_id > others_interactions[j].item_id:
                #if others_interactions[j].rating > cutoff_rating:
                #    others_count += 1
                j+= 1
            else:
                valid_match = 1
                if time_sensitive:
                    if not (my_interactions[i].timestamp > others_interactions[j].timestamp and
                            my_interactions[i].timestamp - others_interactions[j].timestamp <=time_diff):
                        valid_match = 0
                if valid_match == 1:
                    HASH_FIND_INT(count_influencers, &(my_interactions[i].item_id), s)
                    if s == NULL:
                            s = <int_counter *>PyMem_Malloc(cython.sizeof(int_counter))
                            s[0].id = my_interactions[i].item_id
                            s[0].count = 0
                            HASH_ADD_INT_CUSTOM(count_influencers, s)
                    s[0].count += 1
                            
                i += 1
                j += 1
        return count_influencers
    
    cpdef update_items_edge_coverage(self, friends_iterable, int interact_type, unsigned int [:] items_edge_coverage_view, unsigned int [:] items_num_influencers, bool time_sensitive, int time_diff):
        cdef int i
        cdef int_counter *count_influencers = NULL
        cdef int_counter *s = NULL
        for node_obj in friends_iterable:
            c_node_obj = <CNetworkNode>node_obj
            count_influencers = self.update_items_edge_coverage_c(c_node_obj.ndata.c_list[interact_type], c_node_obj.ndata.c_length_list[interact_type], interact_type, count_influencers, time_sensitive, time_diff)
        #Now updating the numpy arrays
        s = count_influencers
        while s!=NULL:
            items_num_influencers[s[0].count] += 1
            items_edge_coverage_view[s[0].id] += 1
            s = <int_counter *>s.hh.next
        delete_hashtable(count_influencers)
        return

    cpdef update_items_popularity(self, interact_type, unsigned int [:] total_popularity_view):
        item_ids = self.get_items_interacted_with(interact_type)
        #print item_ids
        cdef int itemid
        for itemid in item_ids:
            #print itemid
            #if itemid > 11626325: print "oh!"
            total_popularity_view[itemid] += 1

    # By construction, train test are guaranteed to be sorted too
    """
    cpdef create_training_test_sets(self, int interact_type, float traintest_split, float cutoff_rating):
        cdef int i
        cdef int k1 = 0, k2 = 0
        cdef float random_num
        self.c_train_ids = <int *>PyMem_Malloc(cython.sizeof(int) * self.ndata.c_length_list[interact_type])
        self.c_test_ids = <int *>PyMem_Malloc(cython.sizeof(int) * self.ndata.c_length_list[interact_type])
        if not self.c_train_ids or not self.c_test_ids:
            raise MemoryError()

        for i in range(self.ndata.c_length_list[interact_type]):
            if self.ndata.c_list[interact_type][i].rating >= cutoff_rating:
                random_num = (<float>rand())/RAND_MAX
                if random_num < traintest_split:
                    self.c_train_ids[k1] = self.ndata.c_list[interact_type][i].item_id
                    k1 += 1
                else:
                    self.c_test_ids[k2] = self.ndata.c_list[interact_type][i].item_id
                    k2 += 1
        self.ndata.c_length_train_ids = k1
        self.ndata.c_length_test_ids = k2
        #print "train-test numbers", self.ndata.c_length_train_ids, self.ndata.c_length_test_ids, self.uid
    """
    
    # by construction, train, test sets should be sorted by item_id too 
    cpdef create_training_test_sets_bytime(self, int interact_type, int split_timestamp, float cutoff_rating):
        #create train and test set based on a time split
        cdef int i
        cdef int k1 = 0, k2 = 0
        cdef float random_num
        if self.ndata.c_train_data is not NULL:
            PyMem_Free(self.ndata.c_train_data)
        if self.ndata.c_test_data is not NULL:
            PyMem_Free(self.ndata.c_test_data)
        self.ndata.c_train_data = <idata *>PyMem_Malloc(cython.sizeof(idata) * self.ndata.c_length_list[interact_type])
        self.ndata.c_test_data = <idata *>PyMem_Malloc(cython.sizeof(idata) * self.ndata.c_length_list[interact_type])
        if not self.ndata.c_train_data or not self.ndata.c_test_data:
            raise MemoryError()

        for i in range(self.ndata.c_length_list[interact_type]):
            if self.ndata.c_list[interact_type][i].rating >= cutoff_rating:
                if self.ndata.c_list[interact_type][i].timestamp < split_timestamp:
                    self.ndata.c_train_data[k1].item_id = self.ndata.c_list[interact_type][i].item_id
                    self.ndata.c_train_data[k1].rating = self.ndata.c_list[interact_type][i].rating
                    self.ndata.c_train_data[k1].timestamp = self.ndata.c_list[interact_type][i].timestamp
                    k1 += 1
                else:
                    self.ndata.c_test_data[k2].item_id = self.ndata.c_list[interact_type][i].item_id
                    self.ndata.c_test_data[k2].rating = self.ndata.c_list[interact_type][i].rating
                    self.ndata.c_test_data[k2].timestamp = self.ndata.c_list[interact_type][i].timestamp
                    k2 += 1
        self.ndata.c_length_train_ids = k1
        self.ndata.c_length_test_ids = k2
        return
    
    cpdef get_others_prev_actions(self, other_nodes, length_other_nodes, interact_type, timestamp, min_interactions_per_user, time_diff, time_scale, debug=False):
        cdef int j, index
        cdef int size_stream = -1
        cdef int *lengths_others_interactions 
        cdef idata **others_interactions
        cdef idata *others_stream
        cdef int i
        if True:#self.length_fakedata_inf_fr_stream == -1:
            lengths_others_interactions = <int *>PyMem_Malloc(length_other_nodes*cython.sizeof(int))
            others_interactions = <idata **>PyMem_Malloc(length_other_nodes*sizeof(idata *))
            valid_flag = True
            for i in range(length_other_nodes):
                c_node_obj = <CNetworkNode>other_nodes[i]
                lengths_others_interactions[i] = c_node_obj.ndata.c_length_list[interact_type]
                others_interactions[i] = c_node_obj.ndata.c_list[interact_type]
                if lengths_others_interactions[i] < min_interactions_per_user:
                    valid_flag = False
            if not valid_flag:
                print "ALERT: Valid flag is false!!!"
            others_stream = create_nodes_interaction_stream(others_interactions,
                    lengths_others_interactions, length_other_nodes, &size_stream)

            self.fakedata_inf_fr_stream = others_stream
            self.length_fakedata_inf_fr_stream = size_stream
            #PyMem_Free(others_stream)
            PyMem_Free(others_interactions)
            PyMem_Free(lengths_others_interactions)
        fr_interacts = []
        index = binary_search_closest_temporal(self.fakedata_inf_fr_stream, self.length_fakedata_inf_fr_stream, 
                    timestamp, debug)
        if index <-1 or index >= self.length_fakedata_inf_fr_stream:
            if debug:
                print "ERROR:Cython: Big Error, how can index of friend stream be", index
            return None
        elif index == -1:
            if debug:
                print "WARN: Not enough interactions in others test set before this interaction"
            return None
        if index+1<time_diff:
            if debug:
                print "WARN: Few nodes for random sample of influence"
        j=index
        while j >index-time_diff and j>=0:
            fr_interacts.append(self.fakedata_inf_fr_stream[j].item_id)
            j -= 1
        
        """ 
        for j in range(self.length_fakedata_inf_fr_stream):
            sys.stdout.write(str(others_stream[j].item_id)+ ","+str(others_stream[j].timestamp)+" ")
        print "DONEYO"
        """
        PyMem_Free(others_stream)
        return fr_interacts

    def get_train_ids_list(self, int interact_type):
        cdef int i
        itemid_arr = []
        for i in range(self.ndata.c_length_train_ids):
            itemid_arr.append(self.ndata.c_train_data[i].item_id)
        return itemid_arr

    def in_test_set(self, int item_id):
        cdef int i
        for i in range(self.ndata.c_length_test_ids):
            if self.c_test_ids[i] == item_id:
                return True
        return False
    # INFO: deprecated until you update to train_data
    """
    def compute_weighted_popular_recs(self, close_users, int max_users):
        cdef int i
        cdef int_counter *count_items = NULL
        cdef int_counter *s = NULL
        #cdef np.ndarray[DTYPE_INT_t, ndim=1] rec_list_ids = np.zeros(max_users, dtype=DTYPE_INT)
        rec_list_ids = []
        for sim, unode in close_users:
            c_node_obj = <CNetworkNode>unode
            for i in range(c_node_obj.c_length_train_ids):
                if not in_array(c_node_obj.c_train_ids[i], self.c_train_ids, self.ndata.c_length_train_ids):
                    HASH_FIND_INT(count_items, &(c_node_obj.c_train_ids[i]), s)
                    if s == NULL:
                        s = <int_counter *>PyMem_Malloc(cython.sizeof(int_counter))
                        s[0].id = c_node_obj.c_train_ids[i]
                        s[0].count = 0
                        HASH_ADD_INT_CUSTOM(count_items, s)
                    s[0].count += 1
        HASH_SORT(count_items, compare_int_counter)
        s=count_items;
        i = 0
        while s!=NULL and i < max_users:
            rec_list_ids.append(s[0].id)
            i += 1
            s = <int_counter *>s.hh.next
        delete_hashtable(count_items)
        return rec_list_ids
    """

    def get_details(self, interact_type):
        print "Node Id: ", self.ndata.c_uid
        print "Should Have Interactions", self.c_should_have_interactions
        print "Should Have Friends", self.c_should_have_friends
        print "Num Interactions", self.get_num_interactions(interact_type)
        print "Num Friends", self.get_num_friends()


    property uid:
        def __get__(self):
            return self.ndata.c_uid
        def __set__(self, value):
            self.ndata.c_uid = value

    property should_have_interactions:
        def __get__(self):
            return self.c_should_have_interactions

    property should_have_friends:
        def __get__(self):
            return self.c_should_have_friends

    property length_test_ids:
        def __get__(self):
            return self.ndata.c_length_test_ids
    
    property length_train_ids:
        def __get__(self):
            return self.ndata.c_length_train_ids

    #property interactions:
    #    def __get__(self):
    #        return self.ndata.c_list


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

