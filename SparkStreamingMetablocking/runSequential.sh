#!/bin/bash

#D1=inputs/dataset1_imdb
#D2=inputs/dataset2_dbpedia
D1=inputs/dataset1_amazon
D2=inputs/dataset2_gp
Batches=10

mkdir -p outputs/
mkdir -p outputs/sequential/

echo "run sequential"
java -Xmx40g -cp target/prime-1.0-jar-with-dependencies.jar PrimeApproach.PrimeSequential ${D1} ${D2} ${Batches} 1

git
