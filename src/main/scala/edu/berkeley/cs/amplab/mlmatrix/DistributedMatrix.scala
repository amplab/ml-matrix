package edu.berkeley.cs.amplab.mlmatrix

import scala.reflect.ClassTag
import breeze.linalg.DenseMatrix

/**
 * Class representing a DistributedMatrix.
 */
abstract class DistributedMatrix(
    rows: Option[Long] = None,
    cols: Option[Long] = None) extends Serializable {
  //
  // Matrix dimensions
  //
  private var dim_ : (Option[Long], Option[Long]) = (rows, cols)
  protected def getDim: (Long, Long)

  def numRows(): Long = {
    dim._1.get
  }

  def numCols(): Long = {
    dim._2.get
  }

  def length(): Long = {
    dim._1.get * dim._2.get
  }

  def dim: (Option[Long], Option[Long]) = {
    if (dim_._1 == None) {
      val calcDim = getDim
      dim_ = (Some(calcDim._1), Some(calcDim._2))
    }
    dim_
  }

  //
  // Matrix indexing
  //
  // Extract a subset of rows and/or columns
  // def apply(rowRange: Range, colRange: Range): DistributedMatrix
  def apply(rowRange: ::.type, colRange: Range): DistributedMatrix
  def apply(rowRange: Range, colRange: ::.type): DistributedMatrix

  //
  // Element-wise operations
  //
  def +(other: Double): DistributedMatrix = {
    mapElements(x => x + other)
  }

  def -(other: Double): DistributedMatrix = {
    this + (-1 * other)
  }

  def *(other: Double): DistributedMatrix = {
    mapElements(x => x * other)
  }

  def /(other: Double): DistributedMatrix = {
    this * ( 1 / other)
  }

  def pow(other: Double): DistributedMatrix = {
    mapElements(x => math.pow(x, other))
  }

  def mapElements(f: Double => Double): DistributedMatrix

  //
  // Aggregation operations
  //
  def reduceElements(f: (Double, Double) => Double): Double = {
    aggregateElements[Double](0.0)(f, f)
  }

  def aggregateElements[U: ClassTag](zeroValue: U)(seqOp: (U, Double) => U, combOp: (U, U) => U): U

  /**
   * Reduce each row using an associative `f`.  The result matrix has the same number of rows, and 1 column.
   * Example usage includes getting a row sum vector.
   */
  def reduceRowElements(f: (Double, Double) => Double): DistributedMatrix = ???

  /**
   * Reduce each column using an associative `f`.  The result matrix has the same number of columns, and 1 row.
   * Example usage includes getting a column sum vector.
   */
  def reduceColElements(f: (Double, Double) => Double): DistributedMatrix = ???

  /**
   * Returns a row sum vector as a materialized Scala [[Seq]] collected to the driver.  By default,
   * the implementation calls `reduceRowElements`.
   */
  def rowSums(): Seq[Double] = {
    reduceRowElements(_ + _).collect().toArray
  }

  /**
   * Returns a column sum vector as a materialized Scala [[Seq]] collected to the driver.  By default,
   * the implementation calls `reduceColElements`.
   */
  def colSums(): Seq[Double] = {
    reduceColElements(_ + _).collect().toArray
  }

  //
  // Matrix-Matrix operations
  // 
  def +(other: DistributedMatrix): DistributedMatrix
  def -(other: DistributedMatrix): DistributedMatrix = {
    this + (other * -1)
  }

  // def *(other: DenseMatrix[Double]): DistributedMatrix
  // def *(other: DistributedMatrix): DistributedMatrix

  // Back-slash for solve. Default uses QR 
  // def \(other: DistributedMatrix)

  // 
  // Linear Algebra functions
  //
  // Return the frobenius-norm of the matrix
  def normFrobenius(): Double = {
    math.sqrt(mapElements(x => math.pow(x, 2)).reduceElements(_ + _))
  }

  // def norm2(): Double -- Needs SVD

  // Transpose this matrix
  // transpose(): DistributedMatrix


  // Create an Operators class for these
  // qr(): (DistributedMatrix, DistributedMatrix)
  // qrQ(): DistributedMatrix
  // qrR(): DistributedMatrix
  // svd(): (DistributedMatrix, DistributedMatrix, DistributedMatrix)

  // 
  // RDD-like functions
  //
  def cache(): DistributedMatrix
  def collect(): DenseMatrix[Double] 
  // def save(path: String)
}
