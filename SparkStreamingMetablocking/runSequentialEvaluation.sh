#!/bin/bash

GT=inputs/groundtruth_imdbdbpedia
D=outputs/sequential/

mkdir -p outputs/
mkdir -p outputs/sequential/

#inputs/groundtruth_imdbdbpedia outputs/sequential/

echo "run sequential evaluation"
java -cp target/prime-1.0-jar-with-dependencies.jar evaluation.IncrementalQualityEvaluation ${GT} ${D}


