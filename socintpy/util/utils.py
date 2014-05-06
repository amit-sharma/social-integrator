## some utility functions.
import importlib
import math
from threading import Thread
from functools import wraps


def convert_to_utf8_str(arg):
    # written by Michael Norton (http://docondev.blogspot.com/)
    if isinstance(arg, unicode):
        arg = arg.encode('utf-8')
    elif not isinstance(arg, str):
        arg = str(arg)
    return arg


def class_for_name(module_name, class_name):
    # Code from kocikowski's answer at http://stackoverflow.com/a/13808375
    # load the module, will raise ImportError if module cannot be loaded
    m = importlib.import_module(module_name)
    # get the class, will raise AttributeError if class cannot be
    # found
    c = getattr(m, class_name)
    return c


def mean_sd(data_list):
    mean_val = sum(data_list) / float(len(data_list))
    variance = sum([(val - mean_val) * (val - mean_val) for val in data_list]) / float(len(data_list))
    return (mean_val, math.sqrt(variance))


def l2_norm(values_dict, binary=True):
    norm = 0
    if binary:
        norm = len(values_dict)
    else:
        for val in values_dict.itervalues():
            norm += (len(val) * len(val))

    norm = math.sqrt(norm)
    return norm


def swap(t1, t2):
    return t2, t1


def partition(tuple_arr, left, right, pivot_index):
    pivot_value = tuple_arr[pivot_index][1]
    #print pivot_value,"Pivot"
    tuple_arr[pivot_index], tuple_arr[right] = swap(tuple_arr[pivot_index], tuple_arr[right])
    finalpos = 0
    for idx in range(left, right):
        #print tuple_arr[idx][1], "Vals", pivot_value
        if tuple_arr[idx][1] >= pivot_value:
            tuple_arr[idx], tuple_arr[left + finalpos] = swap(tuple_arr[idx], tuple_arr[left + finalpos])
            finalpos += 1
            #print "Oh, no"
    #print finalpos, "FinalPos"
    tuple_arr[right], tuple_arr[left + finalpos] = swap(tuple_arr[right], tuple_arr[left + finalpos])
    return left + finalpos


def selectTopK(tuple_arr, k):
    left = 0
    right = len(tuple_arr) - 1
    while left < right:
        finalpivotpos = partition(tuple_arr, left, right, pivot_index=right)
        #print finalpivotpos
        if finalpivotpos == k - 1 or finalpivotpos == k:
            #print "return"
            return tuple_arr[0:k]
        elif finalpivotpos > k:
            right = finalpivotpos - 1
        else:
            left = finalpivotpos + 1
            #print left, right
    return tuple_arr[0:left + 1]


def run_async(func):
    """
        run_async(func)
            function decorator, intended to make "func" run in a separate
            thread (asynchronously).
            Returns the created Thread object

            E.g.:
            @run_async
            def task1():
                do_something

            @run_async
            def task2():
                do_something_too

            t1 = task1()
            t2 = task2()
            ...
            t1.join()
            t2.join()
    """


    @wraps(func)
    def async_func(*args, **kwargs):
        func_hl = Thread(target=func, args=args, kwargs=kwargs)
        func_hl.start()
        return func_hl

    return async_func


def fast_iter(context, func):
    counter = 0
    for event, elem in context:
        #if counter > 1000:
        #    break
        #counter += 1
        k, v = func(elem)
        #res[k] = v
        while elem.getprevious() is not None:
            del elem.getparent()[0]
        yield k, v

