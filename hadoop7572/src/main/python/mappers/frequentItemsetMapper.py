#!/usr/bin/env python
#used for mapping big datasets, the java PairMapper is just a placeholder to parse the output of this program
__author__ = 'alabdullahwi'
import sys
#init
(last_key, candidates, supportDict, THRESHOLD) = (None, [], {}, 30)

def doCombine(arr):
    retv = []
    if len(arr)==1:
        return retv
    key = "<"
    for i in range(0,len(arr)-1):
        key = key + str(arr[i])+","
        for j in range(i+1, len(arr)):
            retv.append(key+str(arr[j])+">")
    arr.pop(0)
    retv.extend(doCombine(arr))
    return retv

def flush():
    global candidates
    sorted(candidates)
    itemsets = doCombine(candidates)
    for itemset in itemsets:
        try:
            _support = supportDict[itemset]
        except KeyError:
            _support = 0
    _support = _support + 1
    if _support >= THRESHOLD:
        print str(itemset)+"\t1"
    supportDict[itemset] = _support
    candidates = []

for line in sys.stdin:
    tuple = line.split('\t')
    if last_key and last_key != tuple[0]:
      flush()
    last_key=tuple[0]
    if float(tuple[2]) >= 4:
        candidates.append(int(tuple[1]))
flush()











