from matplotlib.pyplot import *
from time import gmtime, strftime
#import math
#import numpy as np
def plotLinesYY(x,y, legendy1, legendy2, labelx="X", labely="Y", display=True):
    listx, listy = (list(t) for t in zip(*sorted(zip(x,y))))
    xlabel(labelx)
    ylabel(labely)

    plot(range(len(listx)), listx, color="blue", label=legendy1)
    plot(range(len(listx)), listy, color="green", label=legendy2)
    legend(loc="upper right")
    if display:
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

def plotHist(vector, labelx="X", labely="Y", logyscale=False):
    hist(vector, bins=100)
    title("Title")
    xlabel(labelx)
    ylabel(labely)
    if logyscale:
        yscale("log")
    show()

def plotCumulativePopularity(items_pop_vector, labelx="X", labely="Y"):
    sorted_vec = sorted(items_pop_vector)
    """
    f = open('graph_itempopularity','r')
    music_line = f.readline()
    #music_arr = [math.log10(int(col.strip("\n"))) for col in music_line.split()]
    music_arr = [int(col.strip("\n")) for col in music_line.split()]
    movies_line = f.readline()
    #movies_arr = [math.log10(int(col.strip("\n"))) for col in movies_line.split()]
    movies_arr = [int(col.strip("\n")) for col in movies_line.split()]
    twitter_line = f.readline()
    #twitter_arr = [math.log10(int(col.strip("\n"))) for col in twitter_line.split()]
    twitter_arr = [int(col.strip("\n")) for col in twitter_line.split()]

    #transform to 100 points -a percentile essentially
    music_arr = sorted(music_arr)
    movies_arr = sorted(movies_arr)
    twitter_arr = sorted(twitter_arr)
    """
    divider = len(sorted_vec)/100
    binned_reads = [sum(sorted_vec[i:i+divider]) for i in range(0, len(sorted_vec), divider)]
    """
    divider = len(music_arr)/100
    binned_music = [sum(music_arr[i:i+divider]) for i in range(0, len(music_arr), divider)]
    divider = len(movies_arr)/100
    binned_movies = [sum(movies_arr[i:i+divider]) for i in range(0, len(movies_arr), divider)]
    divider = len(twitter_arr)/100
    binned_twitter = [sum(twitter_arr[i:i+divider]) for i in range(0, len(twitter_arr), divider)]

    binned_music = getCumulativeArr(binned_music, sum(music_arr))
    binned_movies = getCumulativeArr(binned_movies, sum(movies_arr))
    binned_twitter = getCumulativeArr(binned_twitter, sum(twitter_arr))
    """
    binned_reads = getCumulativeArr(binned_reads, sum(sorted_vec))
    #print len(binned_music), len(binned_movies), len(binned_twitter)
    xlabel("Item percentile")
    ylabel("Cumulative percentage of number of Ratings")
    """
    plot(range(len(music_arr)), sorted(music_arr, reverse=True), color="blue", label="Artists")
    plot(range(len(movies_arr)), sorted(movies_arr, reverse=True), color="green", label="Movies")
    plot(range(len(twitter_arr)), sorted(twitter_arr, reverse=True), color="red", label="Hashtags")
    """
    plot(range(len(binned_reads)), binned_reads, "b-.", label="Last.fm")
    """
    plot(range(len(binned_music)), binned_music, "b-.", label="Artists")
    plot(range(len(binned_movies)), binned_movies, "g-", label="Movies")
    plot(range(len(binned_twitter)), binned_twitter, "r--", label="Hashtags")
    """
    legend(loc = "upper left", bbox_to_anchor=(0.65,0.98))
    #yscale("log")
    #show()
    savefig("cum_plot.png", bbox_inches='tight')

def getCumulativeArr(arr, max_val):
    running_sum =0
    for i in range(len(arr)):
        arr[i] += running_sum
        running_sum = arr[i]
    for i in range(len(arr)):
        arr[i] = arr[i]/float(max_val)*100

    return arr
