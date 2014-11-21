from libcpp.pair cimport pair
from libcpp.map cimport map

cdef int test_stdmap_function():
    cdef map[pair[int,int],int] hasht
    hasht[pair[int,int](1,2)]=2
    print hasht[pair[int,int](1,2)]
    return 1
#test_stdmap_function()

cdef create_efficient_lookup_structure(sim_mat):

    cdef map[pair[int,int],int] hasht
    cdef int i
    dict_sim=sim_mat.data.copy()
    x = sim_mat.row.copy()
    y = sim_mat.col.copy()
    """
    for i in xrange(sim_mat.nnz):
        hasht[pair[int,int](sim_mat.row[i],sim_mat.col[i])]=sim_mat.data[i]
    """
    """
    dict_sim = {}
    dict_sim.update(izip(izip(sim_mat.row,sim_mat.col), sim_mat.data))
    """
    return hasht

cdef create_allusers_interact_mat(nodes, int interact_type, int data_type,
        int num_all_interacts):
    rows = np.zeros(num_all_interacts, dtype=np.intc)
    cols = np.zeros(num_all_interacts, dtype=np.intc)
    values = np.zeros(num_all_interacts, dtype=np.double)
    cdef int idx = 0
    cdef int j, len_interacts
    for node in nodes:
        cnode = <CNetworkNode>node
        j = 0
        ilist = cnode.get_interactions_c(interact_type, &len_interacts, data_type)
        for j in xrange(len_interacts):

            rows[idx] = cnode.uid
            cols[idx] = ilist[j].item_id
            values[idx] = ilist[j].rating
            idx += 1
    return sp.csr_matrix((values, (rows, cols)))

cdef compute_sim_mat(interactions_mat):
    cdef int i,j,k, nrows, ncols, idx
    cdef float t
    nrows, ncols = interactions_mat.shape
    """
    size_entries = nrows*nrows
#cdef np.ndarray[DTYPE_INT_t, ndim=1] indices = np.empty(total_interactions, dtype=DTYPE_INT)
    rows = np.zeros(size_entries, dtype=np.intc)
    cols = np.zeros(size_entries, dtype=np.intc)
    values = np.zeros(size_entries, dtype=np.double)
    int_mat = interactions_mat.tocsr()
    idx = 0
    for i in xrange(nrows):
        for j in xrange(i,nrows):
            t = 0.0
            #for k in xrange(ncols):
            t = int_mat[i].dot(int_mat[j].transpose()).toarray()[0][0]
            if t!=0:
                rows[idx] = i
                cols[idx] = j
                values[idx] = t
                idx += 1
                if j!=i:
                    rows[idx] = j
                    cols[idx] = i
                    values[idx] = t
                    idx += 1
    return sp.coo_matrix((values, (rows, cols))) 
    """
    #int_mat_csr = interactions_mat.tocsr()
    int_mat_t = interactions_mat.transpose(copy=False)
    print "created transpose"
    common_elements_mat =  interactions_mat * int_mat_t
    common_elements_mat = sp.triu(common_elements_mat, k=0, format='csr')
    print common_elements_mat[1:10,1:10]
    print "created user-user common elements matrix", interactions_mat.getnnz(), common_elements_mat.getnnz()

    cdef np.ndarray[DTYPE_INT32_t, ndim=1] sizes_arr = interactions_mat.getnnz(axis =1)
    """
    sizes_mat = np.matrix([np.ones(nrows, dtype=DTYPE_INT), sizes_arr])
    sizes_mat2 = sizes_mat.copy()
    sizes_mat_t = sizes_mat2.transpose()
    sizes_mat_t[:,[0,1]] = sizes_mat_t[:,[1,0]]
    print sizes_mat_t[0:5,:]
    comb_sizes_mat = sizes_mat_t * sizes_mat
    print comb_sizes_mat[0:5,0:10]
   

    print comb_sizes_mat.shape, common_elements_mat.shape
    union_sizes_mat = comb_sizes_mat - common_elements_mat
    #union_sizes_mat = 1 / union_sizes_mat
    print union_sizes_mat[0:10,0:10]
    sparse_union_sizes_mat = sp.csr_matrix(union_sizes_mat)
    #np.seterr(divide='print')
    
    ret = common_elements_mat.multiply(sparse_union_sizes_mat)
    print ret[0:10,0:10]
    #ret = common_elements_mat
    """
    cdef int index, union_size
    common_elements_coo_mat = common_elements_mat.tocoo(copy=False)
    print "Got i, j indices"
        
    common_elements_coo_mat.data = common_elements_coo_mat.data/(sizes_arr[[common_elements_coo_mat.row]] 
            + sizes_arr[[common_elements_coo_mat.col]] - common_elements_coo_mat.data)
    #common_elements_coo_mat.data[common_elements_coo_mat.data <0.0001] = 0 
        #new_V[index] = V[index]/union_size
        #common_elements_mat[I[index], J[index]] /= union_size
    print "Done size computations"
    ret = common_elements_coo_mat
    print "ret is set"
    return ret


def sort_coo(m):
    """
    tuples = izip(m.row, m.col, m.data)
    return tuples.sort(key=lambda x: (x[0], x[1]))
    """
    indices = np.lexsort((m.col,m.row))
    m.row = m.row[indices]
    m.col = m.col[indices]
    m.data = m.col[indices]
    return m 

cpdef compute_allpairs_sim_mat(nodes, interact_type, data_type):
    num_all_interacts = 50000000
    
    interactions_mat = create_allusers_interact_mat(nodes, interact_type, 
            data_type, num_all_interacts)

    print "Read all interacts into a sparse matrix"
    sim_mat = compute_sim_mat(interactions_mat)
    sim_mat = sort_coo(sim_mat)
    #dict_sim = create_efficient_lookup_structure(sim_mat)
    print "computed similarity"
    return sim_mat

cpdef get_similarity_from_mat(int uid1, int uid2, sim_mat):
    cdef int ind1, ind2, temp, low_ind, high_ind
    if uid1 > uid2:
        temp = uid1
        uid1 = uid2
        uid2 = temp
    ind1 = binary_search_standard(sim_mat.row, uid1)
    if ind1 == -1:
        return 0
    i = ind1 - 1
    while sim_mat.row[i] == uid1:
        i-= 1
    low_index = i + 1
    i = ind1 + 1
    while sim_mat.row[i] == uid1:
        i += 1
    high_index = i - 1
    
    ind2 = binary_search_standard(sim_mat.col[low_index:high_index+1], uid2)
    if ind2 == -1:
        return 0
    return sim_mat.data[ind2]
