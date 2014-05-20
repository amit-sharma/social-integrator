from matplotlib.pyplot import *
from time import gmtime, strftime
#import math
#import numpy as np
def plotLinesYY(x,y, legendy1, legendy2, labelx="X", labely="Y"):
    listx, listy = (list(t) for t in zip(*sorted(zip(x,y))))
    xlabel(labelx)
    ylabel(labely)

    plot(range(len(listx)), listx, color="blue", label=legendy1)
    plot(range(len(listx)), listy, color="green", label=legendy2)
    legend(loc="upper right")
    show()
    savefig('plots/'+labelx + "-" + labely + "_" + strftime("%Y-%m-%d%H:%M:%S", gmtime())+".png")

def plotLineY(y, labelx="X", labely="Y", logxscale=False, logyscale=False):
    xlabel(labelx)
    ylabel(labely)
    plot(range(len(y)), y)
    if logxscale:
        xscale("log")
    if logyscale:
        yscale("log")
    show()

def plotHist(vector, labelx="X", labely="Y"):
    hist(vector, bins=100)
    title("Title")
    xlabel(labelx)
    ylabel(labely)
    show()