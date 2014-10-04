package edu.berkeley.cs.amplab.mlmatrix

import org.scalatest.FunSuite
import org.apache.spark.SparkContext

class BlockPartitionedMatrixSuite extends FunSuite with LocalSparkContext with Logging {

  test("reduceRowElements()") {
    sc = new SparkContext("local", "test")
    val matRDD = sc.parallelize(Seq(
      Array[Double](1, 2, 3),
      Array[Double](1, 9, -1),
      Array[Double](0, 0, 1),
      Array[Double](0, 1, 0)
    ), 2) // row-major, laid out as is

    val testMat = BlockPartitionedMatrix.fromArray(matRDD, 1, 1)  // hence each element forms a block
    val testMat2 = BlockPartitionedMatrix.fromArray(matRDD, 2, 1) // each block is 2 by 1
    val testMat3 = BlockPartitionedMatrix.fromArray(matRDD, 2, 3) // each block is 2 by 3
    val rowProducts = testMat.reduceRowElements(_ * _)
    val rowProducts2 = testMat2.reduceRowElements(_ * _)
    val rowProducts3 = testMat3.reduceRowElements(_ * _)

    assert(rowProducts.collect().toArray === Array(6, -9, 0, 0),
      "reduceRowElements() does not return correct answers!")
    assert(rowProducts2.collect().toArray === Array(6, -9, 0, 0),
      "reduceRowElements() does not return correct answers!")
    assert(rowProducts3.collect().toArray === Array(6, -9, 0, 0),
      "reduceRowElements() does not return correct answers!")

    assert(rowProducts.numRows() === 4, "reduceRowElements() returns a result with incorrect row count!")
    assert(rowProducts.numCols() === 1, "reduceRowElements() returns a result with incorrect col count!")
    assert(rowProducts2.numRows() === 4, "reduceRowElements() returns a result with incorrect row count!")
    assert(rowProducts2.numCols() === 1, "reduceRowElements() returns a result with incorrect col count!")
    assert(rowProducts3.numRows() === 4, "reduceRowElements() returns a result with incorrect row count!")
    assert(rowProducts3.numCols() === 1, "reduceRowElements() returns a result with incorrect col count!")
  }

}
