package edu.berkeley.cs.amplab.mlmatrix

import org.scalatest.FunSuite
import org.apache.spark.SparkContext

class BlockPartitionedMatrixSuite extends FunSuite with LocalSparkContext with Logging {

  test("reduceRowElements() and rowSums()") {
    sc = new SparkContext("local", "test")
    val matRDD = sc.parallelize(Seq(
      Array[Double](1, 2, 3),
      Array[Double](1, 9, -1),
      Array[Double](0, 0, 1),
      Array[Double](0, 1, 0)
    ), 2) // row-major, laid out as is
    val expectedRowProducts = Array(6, -9, 0, 0)
    val expectedRowSums = Seq(6, 9, 1, 1)

    val testMat = BlockPartitionedMatrix.fromArray(matRDD, 1, 1)  // hence each element forms a block
    val testMat2 = BlockPartitionedMatrix.fromArray(matRDD, 2, 1) // each block is 2 by 1
    val testMat3 = BlockPartitionedMatrix.fromArray(matRDD, 2, 3) // each block is 2 by 3
    val rowProducts = testMat.reduceRowElements(_ * _)
    val rowProducts2 = testMat2.reduceRowElements(_ * _)
    val rowProducts3 = testMat3.reduceRowElements(_ * _)

    assert(rowProducts.collect().toArray === expectedRowProducts,
      "reduceRowElements() does not return correct answers!")
    assert(rowProducts2.collect().toArray === expectedRowProducts,
      "reduceRowElements() does not return correct answers!")
    assert(rowProducts3.collect().toArray === expectedRowProducts,
      "reduceRowElements() does not return correct answers!")

    assert(rowProducts.numRows() === 4, "reduceRowElements() returns a result with incorrect row count!")
    assert(rowProducts.numCols() === 1, "reduceRowElements() returns a result with incorrect col count!")
    assert(rowProducts2.numRows() === 4, "reduceRowElements() returns a result with incorrect row count!")
    assert(rowProducts2.numCols() === 1, "reduceRowElements() returns a result with incorrect col count!")
    assert(rowProducts3.numRows() === 4, "reduceRowElements() returns a result with incorrect row count!")
    assert(rowProducts3.numCols() === 1, "reduceRowElements() returns a result with incorrect col count!")

    assert(testMat.rowSums() === expectedRowSums, "rowSums() returns incorrect sums!")
    assert(testMat2.rowSums() === expectedRowSums, "rowSums() returns incorrect sums!")
    assert(testMat3.rowSums() === expectedRowSums, "rowSums() returns incorrect sums!")
  }

  test("reduceColElements() and colSums()") {
    sc = new SparkContext("local", "test")
    val matRDD = sc.parallelize(Seq(
      Array[Double](1, 2, 1),
      Array[Double](1, 9, -1),
      Array[Double](1, 0, 1),
      Array[Double](7, 1, 1618)
    ), 1) // row-major, laid out as is
    val expectedColProducts = Array(7, 0, -1618)
    val expectedColSums = Seq(10, 12, 1619)

    val testMat = BlockPartitionedMatrix.fromArray(matRDD, 1, 3)  // each block is one row
    val colProducts = testMat.reduceColElements(_ * _)

    assert(colProducts.collect().toArray === expectedColProducts,
      "reduceRowElements() does not return correct answers!")

    assert(colProducts.numRows() === 1, "reduceColElements() returns a result with incorrect row count!")
    assert(colProducts.numCols() === 3, "reduceColElements() returns a result with incorrect col count!")

    assert(testMat.colSums() === expectedColSums, "colSums() returns incorrect sums!")
  }

}
