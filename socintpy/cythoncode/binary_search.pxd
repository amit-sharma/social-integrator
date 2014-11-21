cimport socintpy.cythoncode.data_types as data_types
from data_types cimport idata, fdata, fsiminfo
from cpython cimport bool
from libc.stdio cimport printf

cdef int in_array(int, int *, int)
cdef int exists(int, fdata *, int)

cdef int binary_search_standard(int [:], int, bool)

cdef int binary_search_closest_temporal(idata *, int, int, bint) nogil
cdef int binary_search_closest(idata *, int, int, bool)
cdef int binary_search_closest_fsiminfo(fsiminfo *, int, float, bint) nogil
cdef int binary_search_timesensitive(idata *, int, int,int, bool)
