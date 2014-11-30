from cpython.mem cimport PyMem_Free

cdef int comp_interactions_temporal(const void *elem1, const void *elem2):
    cdef idata *a
    cdef idata *b
    a = <idata *> elem1
    b = <idata *> elem2
    if a[0].timestamp > b[0].timestamp: return 1
    if a[0].timestamp < b[0].timestamp: return -1
    return 0

cdef int comp_interactions(const void *elem1, const void *elem2):
    cdef idata *a
    cdef idata *b
    a = <idata *> elem1
    b = <idata *> elem2
    if a[0].item_id > b[0].item_id: return 1
    if a[0].item_id < b[0].item_id: return -1
    return 0

cdef int compare_int_counter(const void *elem1, const void *elem2):
    cdef int_counter *a = <int_counter *>elem1
    cdef int_counter *b = <int_counter *>elem2
    if a[0].count > b[0].count: return -1
    if a[0].count < b[0].count: return 1
    return 0

cdef int comp_friends(const void *elem1, const void *elem2):
    cdef fdata *a
    cdef fdata *b
    a = <fdata *> elem1
    b = <fdata *> elem2
    if a[0].receiver_id > b[0].receiver_id: return 1
    if a[0].receiver_id < b[0].receiver_id: return -1
    return 0

cdef int comp_floats(const void *elem1, const void *elem2):
    cdef float *a
    cdef float *b
    a = <float *>elem1
    b = <float *>elem2
    return <int>(a[0] - b[0])

cdef int comp_fsiminfo(const void *elem1, const void *elem2):
    cdef fsiminfo *a
    cdef fsiminfo *b
    a = <fsiminfo *>elem1
    b = <fsiminfo *>elem2
    if a[0].sim > b[0].sim: return 1
    if a[0].sim < b[0].sim: return -1
    return 0
    #return <int>(a[0].sim - b[0].sim)

cdef inline void swap_elements(int *a, int *b) nogil:
    cdef int t = a[0]
    a[0]=b[0]
    b[0]=t
    return

cdef void delete_hashtable(int_counter *head_ptr):
    cdef int_counter *s
    cdef int_counter *tmp

    s = head_ptr
    while s != NULL:
        HASH_DEL(head_ptr,s)
        tmp = <int_counter *>s.hh.next
        PyMem_Free(s)
        s = tmp
    return 
