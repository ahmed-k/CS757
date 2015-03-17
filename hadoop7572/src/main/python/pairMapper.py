#!/usr/bin/env python
#used for mapping big datasets, the java PairMapper is just a placeholder to parse the output of this program
import sys
#total is the number of files
(last_key, candidates) = (None, [])
for line in sys.stdin:

 tuple = line.split('\t')
 if last_key and last_key != tuple[0]:
   for i in range(0, len(candidates)-1):
    for j in range(i, len(candidates)):
     print str(candidates[i])+"_"+str(candidates[j])+"\t1"
   candidates=[]
 last_key=tuple[0]
 if float(tuple[2]) >= 4:
   candidates.append(int(tuple[1]))
for i in range(0, len(candidates)-1):
 for j in range(i, len(candidates)):
  print str(candidates[i])+"_"+str(candidates[j])+"\t1"