CS757
Assignment 3 : Recommender System Using Batch Gradient Descent
Ahmed Alabdullah
README FILE


TABLE OF CONTENTS

1. Overview 
2. Starting The Program 
3. Pseudocode
4. Comments On Output


##1 OVERVIEW


The way my program works is as follows:

The main component is a jar file, which runs on hadoop and expects two files:

1. a file containing the U and V matrices, upon which it will run batch gradient descent
2. a file containing the dataset


the jar will first:
  first:
1. compute batch gradient descent for both U and V matrix, implementing the algorithm described in the book. this job is configured in the main method of the AssignmentDriver class, it gets configured by the JobFactory class's configureBGD method,  which will produce this job.
When the job runs on the cluster, it will terminate in a new UV_matrices output file, containing the new updates values for both      

Then a second job will run in the main method, which will compare the values of the new UV_matrices output file against the old one for each matrix cell entry, to determine if there is a margin of difference greater than some threshold (it was set to 0.3 in the code) this is my step function. It will produce a boolean for every cell in the matrix: true meaning that the difference is greater than the threshold and thus another iteration of BGD is needed. 

all the boolean values from the second job's output are collpased into a single boolean by a third job (CollapseBoolean) which will produce a single boolean 
value, true or false, it is the result of ORing the entire boolean values of the second job. If true, another iteration of BGD is needed. false meaning none is needed. 

This value is examined by the bash script, runBGD.sh, which provides the main iteration facility in this program. If the value read is true, it will rerun the hadoop jar again, going through all three jobs again.
if false, it multiply the resultant UV_matrices, producing the prediction matrix
This is the final output of the program.

#2 STARTING THE PROGRAM:
	
the batch script runBGD.sh will prepare the necessary files, run the hadoop job, look at the output boolean value to determine if another iteration is needed, and if so run BGD once more. If not needed, it will run a matrix multiplication mapreduce job to produce the final prediction matrix.      

the hadoop job expects three parameters: the dimensions of the matrix which has to be known well in advance. 
suppose the original dataset is an m by n matrix. So our U matrix becomes an m by d matrix, and the V matrix is a d by n matrix. The parameters to the hadoop job will be : "[value of m] [value of n] [value of d]"  


##3: Pseudocode

Please refer to the separate file pseudocode.txt in the documentation folder.


##4: Comments on Output

The predication matrix that results does not seem to be accurate. the numbers are outside the expected range of values for a cell ( 1 to 5). I ran out of time before I could find the bug that caused this. Although when I ran it on a small matrix for a single time it produced the results I was expecting.    








