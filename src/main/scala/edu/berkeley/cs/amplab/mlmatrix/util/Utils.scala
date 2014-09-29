package edu.berkeley.cs.amplab.mlmatrix.util

import org.apache.spark.rdd.RDD
import breeze.linalg._

object Utils {

  /**
   * Deep copy a Breeze matrix
   */
  def cloneMatrix(in: DenseMatrix[Double]) = {
    val arrCopy = new Array[Double](in.rows * in.cols)
    System.arraycopy(in.data, 0, arrCopy, 0, arrCopy.length)
    val out = new DenseMatrix[Double](in.rows, in.cols, arrCopy)
    out
  }

}
