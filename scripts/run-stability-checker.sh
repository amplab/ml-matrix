#!/usr/bin/env bash

#SPARK_MASTER=`cat /root/spark-ec2/cluster-url`
SPARK_MASTER="local"

#NUM_ROWS=1000000
NUM_ROWS=1000
NUM_COLS=10
NUM_PARTITIONS=2
NUM_CLASSES=1

SGD_STEP_SIZE=0.1
SGD_ITERATIONS=10
SGD_BATCH_FRACTION=1.0

if [[ $# -ne 1 ]]; then
  echo "Usage: run-stability-checker.sh <suffix>"
  exit 0;
fi

./run-main.sh edu.berkeley.cs.amplab.mlmatrix.StabilityChecker $SPARK_MASTER $NUM_ROWS $NUM_COLS $NUM_PARTITIONS $NUM_CLASSES tsqr >& logs-tsqr-$1.txt

./run-main.sh edu.berkeley.cs.amplab.mlmatrix.StabilityChecker $SPARK_MASTER $NUM_ROWS $NUM_COLS $NUM_PARTITIONS $NUM_CLASSES normal >& logs-normal-$1.txt

./run-main.sh edu.berkeley.cs.amplab.mlmatrix.StabilityChecker $SPARK_MASTER $NUM_ROWS $NUM_COLS $NUM_PARTITIONS $NUM_CLASSES sgd $SGD_STEP_SIZE $SGD_ITERATIONS $SGD_BATCH_FRACTION >& logs-sgd-$1.txt
