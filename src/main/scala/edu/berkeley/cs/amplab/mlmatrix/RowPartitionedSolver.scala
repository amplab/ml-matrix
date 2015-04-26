package edu.berkeley.cs.amplab.mlmatrix

import breeze.linalg._

import org.apache.spark.rdd.RDD

abstract class RowPartitionedSolver {

  def solveLeastSquares(
      A: RowPartitionedMatrix,
      b: RowPartitionedMatrix): DenseMatrix[Double] = {
    solveLeastSquaresWithL2(A, b, 0.0)
  }

  def solveLeastSquaresWithL2(
      A: RowPartitionedMatrix,
      b: RowPartitionedMatrix,
      lambda: Double): DenseMatrix[Double] = {
    solveLeastSquaresWithManyL2(A, b, Array(lambda)).head
  }

   /**
    * Solves a single least squares problem with l2 regularization using
    * several different lambdas as regularization parameters
    */
  def solveLeastSquaresWithManyL2(
      A: RowPartitionedMatrix,
      b: RowPartitionedMatrix,
      lambdas: Array[Double]): Seq[DenseMatrix[Double]]

   /**
    * Solves several least squares problems (by varying the b vector and keeping
    * the A matrix constant) with l2 regularization using different lambdas as
    * regularization parameters.
    */
  // TODO: This interface should ideally take in Seq[RowPartitionedMatrix] ?
  def solveManyLeastSquaresWithL2(
      A: RowPartitionedMatrix,
      b: RowPartitionedMatrix,
      residuals: RDD[Array[DenseMatrix[Double]]],
      lambdas: Array[Double]): Seq[DenseMatrix[Double]]

}
