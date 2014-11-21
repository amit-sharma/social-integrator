#!python
#cython: boundscheck=False, wraparound=False, cdivision=True

cdef int in_array(int val, int *arr, int length_arr):
    cdef int low, high, mid
    low = 0
    high = length_arr -1

    while(low <= high):
        mid = (low + high)/2
        if val == arr[mid]:
            return 1
        elif val > arr[mid]:
            low = mid+1
        else:
            high = mid-1
    return 0

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

cdef int binary_search_standard(int [:] arr, int value, bool debug):
    cdef int lower = 0
    cdef int upper = arr.shape[0] - 1
    cdef int mid = -1
    cdef int item_index = -1
    cdef int num_iter = 0
    while lower <= upper:
        mid = (lower+upper)/2
        if debug:
            print lower, mid, upper
        if arr[mid] == value:
            item_index = mid
            break
        elif arr[mid] > value:
            upper = mid - 1
        else:
            lower = mid + 1
        num_iter += 1
    print num_iter, arr.shape[0], "Iterations to find the i or j"
    if debug:
        print arr[mid]
    if item_index != -1:
        return item_index
    return -1

cdef int binary_search_closest_temporal(idata *interactions, int length_interactions, 
        int timestamp,
        bint debug) nogil:
    cdef int lower = 0
    cdef int upper = length_interactions - 1
    cdef int mid = -1
    cdef int item_index = -1
    while lower <= upper:
        mid = (lower+upper)/2
        if debug:
            printf("%d %d %d", lower, mid, upper)
        if interactions[mid].timestamp == timestamp:
            item_index = mid
            break
        elif interactions[mid].timestamp > timestamp:
            upper = mid - 1
        else:
            lower = mid + 1
    if debug:
        printf("%d", interactions[mid].timestamp)
    if item_index != -1:
        return item_index
    elif upper < mid:
        return mid - 1 
    elif lower > mid:
        return mid # returning the first index whose value is lesser than the item_id
    return -1

cdef int binary_search_closest(idata *interactions, int length_interactions, 
        int item_id,
        bool debug):
    cdef int lower = 0
    cdef int upper = length_interactions - 1
    cdef int mid = -1
    cdef int item_index = -1
    while lower <= upper:
        mid = (lower+upper)/2
        if debug:
            print lower, mid, upper
        if interactions[mid].item_id == item_id:
            item_index = mid
            break
        elif interactions[mid].item_id > item_id:
            upper = mid - 1
        else:
            lower = mid + 1
    if debug:
        print interactions[mid].item_id
    if item_index != -1:
        return item_index
    elif upper < mid:
        return mid
    elif lower > mid:
        return mid + 1 # returning the first index whose value is greater than the item_id
    return -1

#TODO return the lower one, or check all
cdef int binary_search_closest_fsiminfo(fsiminfo *interactions, int length_interactions, 
        float sim,
        bint debug) nogil:
    cdef int lower = 0
    cdef int upper = length_interactions - 1
    cdef int mid = -1
    cdef int item_index = -1
    while lower <= upper:
        mid = (lower+upper)/2
        if debug:
            printf("%d %d %d", lower, mid, upper)
        if interactions[mid].sim == sim:
            item_index = mid
            break
        elif interactions[mid].sim > sim:
            upper = mid - 1
        else:
            lower = mid + 1
    if debug:
        printf("%f", interactions[mid].sim)
    if item_index != -1:
        return item_index
    elif upper < mid:
        return mid
    elif lower > mid:
        return mid + 1 # returning the first index whose value is greater than the item_id
    return -1


cdef int binary_search_timesensitive(idata *interactions, int length_interactions, int item_id, int
        timestamp, bool debug):
    cdef int lower = 0
    cdef int upper = length_interactions - 1
    cdef int mid = -1
    cdef int item_index = -1
    while lower <= upper:
        mid = (lower+upper)/2
        if debug:
            print lower, mid, upper
        if interactions[mid].item_id == item_id:
            item_index = mid
            break
        elif interactions[mid].item_id > item_id:
            upper = mid - 1
        else:
            lower = mid + 1
    if debug:
        print interactions[mid].item_id
    if item_index != -1:
        if interactions[item_index].timestamp == timestamp:
            return item_index
        else:
            i = item_index + 1
            while interactions[i].item_id == item_id:
                if interactions[i].timestamp == timestamp:
                    return i
                i += 1
            
            i = item_index - 1
            while interactions[i].item_id == item_id:
                if interactions[i].timestamp == timestamp:
                    return i
                i -= 1
        if debug:
            print "Found but not matching timestamp"
    return -1
