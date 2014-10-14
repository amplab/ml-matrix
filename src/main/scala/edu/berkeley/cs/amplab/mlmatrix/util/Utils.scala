package edu.berkeley.cs.amplab.mlmatrix.util

import scala.reflect.ClassTag

import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import breeze.linalg._

object Utils {

  /**
   * Deep copy a Breeze matrix
   */
  def cloneMatrix(in: DenseMatrix[Double]) = {
    // val arrCopy = new Array[Double](in.rows * in.cols)
    // System.arraycopy(in.data, 0, arrCopy, 0, arrCopy.length)
    val out = new DenseMatrix[Double](in.rows, in.cols)
    var r = 0
    while (r < in.rows) {
      var c = 0
      while (c < in.cols) {
        out(r, c) = in(r, c)
        c = c + 1
      }
      r = r + 1
    }
    out
  }

  /**
   * Reduces the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#reduce]]
   */
  def treeReduce[T: ClassTag](rdd: RDD[T], f: (T, T) => T, depth: Int = 2): T = {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(f))
      } else {
        None
      }
    }
    val partiallyReduced = rdd.mapPartitions(it => Iterator(reducePartition(it)))
    val op: (Option[T], Option[T]) => Option[T] = (c, x) => {
      if (c.isDefined && x.isDefined) {
        Some(f(c.get, x.get))
      } else if (c.isDefined) {
        c
      } else if (x.isDefined) {
        x
      } else {
        None
      }
    }
    treeAggregate(Option.empty[T])(partiallyReduced, op, op, depth)
      .getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
   * Aggregates the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#aggregate]]
   */
  def treeAggregate[T: ClassTag, U: ClassTag](zeroValue: U)(
      rdd: RDD[T],
      seqOp: (U, T) => U,
      combOp: (U, U) => U,
      depth: Int = 2): U = {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    if (rdd.partitions.size == 0) {
      return zeroValue
    }
    val aggregatePartition = (it: Iterator[T]) => it.aggregate(zeroValue)(seqOp, combOp)
    var partiallyAggregated = rdd.mapPartitions(it => Iterator(aggregatePartition(it)))
    var numPartitions = partiallyAggregated.partitions.size
    val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)
    // If creating an extra level doesn't help reduce the wall-clock time, we stop tree aggregation.
    while (numPartitions > scale + numPartitions / scale) {
      numPartitions /= scale
      val curNumPartitions = numPartitions
      partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex { (i, iter) =>
        iter.map((i % curNumPartitions, _))
      }.reduceByKey(new HashPartitioner(curNumPartitions), combOp).values
    }
    partiallyAggregated.reduce(combOp)
  }

  def aboutEq(a: DenseMatrix[Double], b: DenseMatrix[Double], thresh: Double = 1e-8) = {
    math.abs(max(a-b)) < thresh
  }
}
