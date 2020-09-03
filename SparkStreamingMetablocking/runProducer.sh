#!/bin/bash

ProdAddr=localhost:9092
ProdTime=600
D1=inputs/dataset1_imdb
D2=inputs/dataset2_dbpedia

#ProdTime=300
#D1=inputs/dataset1_amazon
#D2=inputs/dataset2_gp

Batches=10

mkdir -p outputs/
mkdir -p checkpoint/

echo "run producer"
#java -cp target/prime-1.0-jar-with-dependencies.jar KafkaIntegration.KafkaDataStreamingProducerIntervalBatch ${ProdAddr} ${ProdTime} ${D1} ${D2} ${Batches}
java -cp target/prime-1.0-jar-with-dependencies.jar PrimeApproach.KafkaBatchProducerSynch ${ProdAddr} ${ProdTime} ${D1} ${D2} ${Batches}


