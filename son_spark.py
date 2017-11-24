from pyspark import SparkContext
from collections import defaultdict
from operator import add
from itertools import combinations
import sys

sc = SparkContext(appName="inf553")

support_ratio = 0.3

def fx(x):
    a = list(x[0])
    return a

def findCandidates(inp, length):
    candidate = []

    for i in xrange(0,len(inp)):
        for j in xrange(i+1,len(inp)):
            #print set(inp[i]).union(set(inp[j]))
            if(len(set(inp[i]).union(set(inp[j])))==length):
                if not candidate.__contains__(tuple(set(inp[i]).union(set(inp[j])))):
                    candidate.append(tuple(set(inp[i]).union(set(inp[j]))))

    return candidate

def partitionApriori(x):
    test = list(x)
    outputlist = []
    support = support_ratio*len(test)
    #print support
    stop = 0
    inp=[]
    for i in xrange(0, len(test)):
        inp = set(inp).union(set(test[i]))
        if stop < len(test[i]):
            stop = len(test[i])
    inp = list(inp)
    #print inp
    #print stop

    for i in xrange(0,len(inp)):
        count = 0
        for j in xrange(0,len(test)):
            if set(test[j]).__contains__(inp[i]):
                count += 1

        if count >= support:
            outputlist.append(inp[i])
    #1-frequent count complete

    # for len 2
    temp_list = []
    inp  = list(combinations(list(inp),2))
    for i in xrange(0,len(inp)):
        count = 0;
        for j in xrange(0, len(test)):
            if len(set(combinations(list(inp[i]), 2)).intersection(set(combinations(test[j], 2))))==1:
                count = count+1

        if count >= support:
            outputlist.append(inp[i])
            temp_list.append(inp[i])

    inp = temp_list
    temp_list = []
    l = 3
    #stop = 6

    while (l <= stop and len(inp) > 1):
        output = findCandidates(inp, l)
        inp = output
        temp_list = []

        print l
        print inp

        for i in xrange(0, len(inp)):
            count = 0;
            for j in xrange(0, len(test)):
                if len(set(combinations(list(inp[i]), l)).intersection(set(combinations(test[j], l)))) == 1:
                    count = count + 1

            # print str(inp[i])+":"+str(count)
            if count >= support:
                # local_dict[str(inp[i])]=count
                temp_list.append(inp[i])
                outputlist.append(inp[i])

        l += 1
        inp = temp_list
        print inp

    return outputlist

def main():
	inpRDD = sc.textFile(sys.argv[1],2)
	output = inpRDD.map(lambda x:x.split("\n"))

	getList = output.mapPartition(partitionApriori)

	print output.collect()
	print output.getNumPartitions()

if __name__ == "__main__":
	main()