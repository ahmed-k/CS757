#!/bin/bash
# runs Batch Gradient Descent 
hadoop fs -rm -R output/*
hadoop fs -rm -R input/* 
hadoop fs -put UV_matrices.dat input 
hadoop fs -put five_by_five_matrix.dat input 
ITERATION="false"
while [ $ITERATION != "true" ] 
do 
echo "Running Batch Gradient Descent..."
time hadoop jar CS757/HW3/target/homework3-1.0-SNAPSHOT.jar drivers/AssignmentDriver input output/bgd $1 $2 $3 
hadoop fs -get output/collapse/part-r-00000
ITERATION=$(<part-r-0000)
hadoop fs -rm -R input/* 
hadoop fs -put five_by_five_matrix.dat input 
hadoop fs -mv output/bgd/part* input
hadoop fs -rm -R output
done 
hadoop fs -rm -R input/* 
hadoop fs -mv output/bgd/part* input
time hadoop jar CS757/HW3/target/homework3-1.0-SNAPSHOT.jar drivers/AssignmentDriver input output/final $1 $2 $3 mul 
 

