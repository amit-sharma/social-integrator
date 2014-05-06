import cython

cdef class CNetworkNode:
    cdef public int _uid
    cdef public int _has_friends
    cdef public int _has_interactions

    property uid:
        def __get__(self):
            return self.uid

    property has_interactions:
        def __get__(self):
            return self.has_interactions

    property has_friends:
        def __get__(self):
            return self.has_friends