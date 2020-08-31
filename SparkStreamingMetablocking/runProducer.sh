#!/bin/bash

ProdAddr=localhost:9092
ProdTime=15
D1=inputs/dataset1_abt
D2=inputs/dataset2_buy
Batches=10

mkdir -p outputs/
mkdir -p checkpoint/

echo "run producer"
#java -cp target/prime-1.0-jar-with-dependencies.jar KafkaIntegration.KafkaDataStreamingProducerIntervalBatch ${ProdAddr} ${ProdTime} ${D1} ${D2} ${Batches}
java -cp target/prime-1.0-jar-with-dependencies.jar PrimeApproach.KafkaBatchProducerSynch ${ProdAddr} ${ProdTime} ${D1} ${D2} ${Batches}


