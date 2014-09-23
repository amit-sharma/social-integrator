import numpy as np
import operator
from network_analyzer_example import *
import compare_adopt_share
from compare_adopt_share import *
import socintpy.util.plotter as plotter

if __name__ == "__main__":
    data = get_data()

na = AdoptShareComparer(data)
