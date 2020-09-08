#!/bin/bash

D1=inputs/dataset1_imdb
D2=inputs/dataset2_dbpedia
#D1=inputs/dataset1_amazon
#D2=inputs/dataset2_gp
nBatches=(1 10 20 50 100)

mkdir -p outputs/
mkdir -p outputs/sequential/

for Batches in "${nBatches[@]}"
do
  mkdir -p outputs/sequential/b${Batches}
  mkdir -p outputs/b${Batches}
  echo "run sequential"
  java -Xmx40g -cp target/prime-1.0-jar-with-dependencies.jar PrimeApproach.PrimeSequential ${D1} ${D2} ${Batches} 1
done




