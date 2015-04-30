#!/bin/bash
# runs Batch Gradient Descent 
hadoop fs -rm -R output/*
hadoop fs -rm -R input/* 
hadoop fs -put UV_matrices.dat input 
hadoop fs -put original_matrix.dat input
ITERATION="false"
ITERATION_NUM=0
while [ "$ITERATION" != "true" ] 
do 
ITERATION_NUM=$((ITERATION_NUM+1))
echo "Running Batch Gradient Descent...run number $ITERATION_NUM"
time hadoop jar CS757/HW3/target/homework3-1.0-SNAPSHOT.jar drivers/AssignmentDriver input output/bgd $1 $2 $3 
rm part-r-00000
hadoop fs -get output/collapse/part-r-00000
ITERATION=$(<part-r-00000)
echo $ITERATION
hadoop fs -rm -R input/* 
hadoop fs -put five_by_five_matrix.dat input 
hadoop fs -mv output/bgd/part* input/UV_matrices.dat
hadoop fs -rm -R output
done 
time hadoop jar CS757/HW3/target/homework3-1.0-SNAPSHOT.jar drivers/AssignmentDriver input output/final $1 $2 $3 mul 
 

