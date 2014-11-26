
cdef extern from "uthash.h":
    ctypedef struct UT_hash_table:
        pass

    cdef struct UT_hash_handle:
        UT_hash_table *tbl
        void *prev
        void *next
        UT_hash_handle *hh_prev
        UT_hash_handle *hh_next
        void *key
        unsigned keylen
        unsigned hashv

    cdef void HASH_FIND_INT(int_counter *head_ptr, int *query_id, int_counter *s)
    cdef void HASH_ADD_INT_CUSTOM(int_counter *head_ptr, int_counter *s)
    cdef void HASH_SORT(int_counter *head_ptr, int(const void *, const void *))
    cdef void HASH_DEL(int_counter *head_ptr, int_counter *s)

cdef struct netnodedata:
    int c_uid
    idata **c_list             # interactions array for each interact type
    int *c_length_list         # length of the interactions in each type
    
    fdata *c_friend_list       # friends array
    int c_length_friend_list   # length of friends array
    
    # Train and testing data. Temporary buffers, used when required.
    idata *c_train_data
    idata *c_test_data
    int c_length_train_ids
    int c_length_test_ids

cdef struct idata:
    int item_id
    #char *timestamp
    unsigned int timestamp
    int rating

cdef struct fdata:
    int receiver_id

ctypedef struct int_counter:
    int id
    int count
    UT_hash_handle hh

cdef struct nodesusceptinfo:
    float fsim
    float rsim
    float fsusc
    float rsusc

cdef struct fsiminfo:
    float sim
    int uid

cdef struct loopvars:
    int counter
    int counter2
    int count_success
    int eligible_nodes_counter
    int node_fnode_counter
    int node_rnode_counter

cdef struct compfrnonfr:
    float fsim
    float rsim
    netnodedata fnode
    netnodedata rnode

cdef struct twoints:
    int val1
    int val2

cdef int comp_interactions_temporal(const void *, const void *)
cdef int comp_interactions(const void *, const void *)
cdef int compare_int_counter(const void *, const void *)
cdef int comp_friends(const void *, const void *)
cdef int comp_fsiminfo(const void *, const void *)

    
cdef inline int int_min(int a, int b) nogil: return a if a <= b else b
cdef inline void swap_elements(int *, int *) nogil

cdef void delete_hashtable(int_counter *)
