#!/usr/bin/env bash
echo "Starting parameter sweep"

#SPARK_HOME="/root/spark"
#SPARK_HOME="/home/shivaram/projects/spark"
SPARK_HOME="/Users/becca/git/spark"

#SPARK_MASTER=`cat /root/spark-ec2/cluster-url`
SPARK_MASTER="local"

#NUM_ROWS=1000000
NUM_ROWS=1000
NUM_COLS=10
NUM_PARTITIONS=2
NUM_CLASSES=1

if [[ $# -ne 1 ]]; then
  echo "Usage: thetaCondSweep.sh <suffix>"
  exit 0;
fi

#for THETA in 0 0.25 0.5 0.75 1 1.25 1.5; do
#  for COND_NUMBER in 1 10 100 1000 10000 100000; do
#    echo "Runing test for theta ="$THETA" and condNumber = "$COND_NUMBER
#for iter in `seq 80 80`
#do
  #./run-main.sh edu.berkeley.cs.amplab.mlmatrix.StabilityChecker $SPARK_MASTER $SPARK_HOME $NUM_ROWS $NUM_COLS $NUM_PARTITIONS $NUM_CLASSES normal 5.0 40 > logs-normal-$1.txt
  ./run-main.sh edu.berkeley.cs.amplab.mlmatrix.StabilityChecker $SPARK_MASTER $SPARK_HOME $NUM_ROWS $NUM_COLS $NUM_PARTITIONS $NUM_CLASSES tsqr 5.0 40 > logs-tsqr-$1.txt
  #./run-main.sh edu.berkeley.cs.amplab.mlmatrix.StabilityChecker $SPARK_MASTER $SPARK_HOME $NUM_ROWS $NUM_COLS $NUM_PARTITIONS $NUM_CLASSES local 5.0 40 > logs-local-$1.txt
  #./run-main.sh edu.berkeley.cs.amplab.mlmatrix.StabilityChecker $SPARK_MASTER $SPARK_HOME $NUM_ROWS  $NUM_COLS $NUM_PARTITIONS $NUM_CLASSES blockcd 5.0 10 > logs-blockcd-$1-10.txt
  #./run-main.sh edu.berkeley.cs.amplab.mlmatrix.StabilityChecker $SPARK_MASTER $SPARK_HOME $NUM_ROWS  $NUM_COLS $NUM_PARTITIONS $NUM_CLASSES blockcd 5.0 100 > logs-blockcd-$1-100.txt
  #./run-main.sh edu.berkeley.cs.amplab.mlmatrix.StabilityChecker $SPARK_MASTER $SPARK_HOME $NUM_ROWS $NUM_COLS $NUM_PARTITIONS $NUM_CLASSES sgd 1.0 100 > logs-sgd-$1-1-100.txt

  #cat logs-normal-$1.txt | grep "err" | awk '{print $NF" "$(NF-1)}' > logs-normal-vander-$1.txt
  cat logs-tsqr-$1.txt | grep "err" | awk '{print $NF" "$(NF-1)}' > logs-tsqr-vander-$1.txt
  #cat logs-local-$1.txt | grep "err" | awk '{print $NF" "$(NF-1)}' > logs-local-vander-$1.txt
  #cat logs-blockcd-$1-10.txt | grep "err" | awk '{print $NF" "$(NF-1)}' > logs-blockcd-vander-$1-10.txt
  #cat logs-blockcd-$1-100.txt | grep "err" | awk '{print $NF" "$(NF-1)}' > logs-blockcd-vander-$1-100.txt
  #cat logs-sgd-$1-1-100.txt | grep "err" | awk '{print $NF" "$(NF-1)}' > logs-sgd-vander-$1-1-100.txt
#done
