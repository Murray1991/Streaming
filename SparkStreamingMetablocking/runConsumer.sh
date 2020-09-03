#!/bin/bash

ProdAddr=localhost:9092

#ConsAddr=localhost:9092
WinTime=20
#Args(1) == ProdAddr
WinSlidingTime=80
ConsOutput=outputs/movies/test.txt
#ConsOutput=outputs/agp/test.txt

ConsCheckpoint=checkpoint/
ConsNodes=1

mkdir -p outputs/
mkdir -p outputs/movies
mkdir -p outputs/agp
mkdir -p checkpoint/
mkdir -p outputs/old

cp -r outputs/movies/ outputs/old/
cp -r outputs/agp/ outputs/old/

rm  outputs/movies/*
rm  outputs/agp/*

echo "run consumer"
#java -cp target/prime-1.0-jar-with-dependencies.jar PrimeApproach.PRIMEStructuredAndMatching ${ConsTime} ${ProdAddr} ${ConsTime2} ${ConsOutput} ${ConsCheckpoint} ${ConsNodes}

current_dir=.
log4j_setting="-Dlog4j.configuration=log4j.properties"

spark-submit \
  --conf "spark.driver.extraJavaOptions=-Dlog4jspark.root.logger=OFF,console" \
  --class PrimeApproach.PRIMEStructured \
  --master local[1] \
  --deploy-mode client \
  --executor-memory 40G \
  --num-executors 1 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 \
  target/prime-1.0-jar-with-dependencies.jar \
  ${WinTime} ${ProdAddr} ${WinSlidingTime} ${ConsOutput} ${ConsCheckpoint} ${ConsNodes}

#spark-submit \
#  --conf "spark.driver.extraJavaOptions=-Dlog4jspark.root.logger=OFF,console" \
#  --class PrimeApproach.PRIMEStructuredAndMatching \
#  --master local[1] \
#  --deploy-mode client \
#  --executor-memory 8G \
#  --num-executors 1 \
#  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 \
#  target/prime-1.0-jar-with-dependencies.jar \
#  ${WinTime} ${ProdAddr} ${WinSlidingTime} ${ConsOutput} ${ConsCheckpoint} ${ConsNodes}


#./bin/spark-submit \
#  --class <main-class> \
#  --master <master-url> \
#  --deploy-mode <deploy-mode> \
#  --conf <key>=<value> \
#  ... # other options
#  <application-jar> \
#  [application-arguments]