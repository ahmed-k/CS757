#!/bin/bash
#master script: downloads repo from GitHub, compiles using Maven, runs a MR job, get results from output, then vim into the result
./scripts/git.sh
./scripts/compile.sh
./scripts/run.sh $1 $2
./scripts/get.sh
vim output/part-r-00000