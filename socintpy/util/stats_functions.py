from scipy.stats import ttest_rel

def do_paired_t_test(arr1, arr2):
    return ttest_rel(arr1, arr2)
