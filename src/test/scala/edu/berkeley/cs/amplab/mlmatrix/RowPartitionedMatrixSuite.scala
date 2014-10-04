package edu.berkeley.cs.amplab.mlmatrix

import org.scalatest.FunSuite
import org.apache.spark.SparkContext

class RowPartitionedMatrixSuite extends FunSuite with LocalSparkContext with Logging {

  test("reduceRowElements()") {
    sc = new SparkContext("local", "test")
    val testMat = RowPartitionedMatrix.fromArray(
      sc.parallelize(Seq(
        Array[Double](1, 2, 3),
        Array[Double](1, 9, -1),
        Array[Double](0, 0, 1),
        Array[Double](0, 1, 0)
      ), 2), // row-major, laid out as is
      Seq(2, 2),
      3
    )
    val rowProducts = testMat.reduceRowElements(_ * _).collect().toArray
    assert(rowProducts === Array(6, -9, 0, 0), "reduceRowElements() does not return correct answers!")
  }

  test("rowSums()") {
    sc = new SparkContext("local", "test")
    val testMat = RowPartitionedMatrix.fromArray(
      sc.parallelize(Seq(
        Array[Double](1, 2, 3),
        Array[Double](1, 9, -1),
        Array[Double](0, 0, 1),
        Array[Double](0, 1, 0)
      ), 4), // row-major, laid out as is
      Seq(1, 1, 1, 1),
      3
    )
    assert(testMat.rowSums() === Seq(6, 9, 1, 1), "rowSums() returns incorrect sums!")
  }

}
