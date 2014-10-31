package edu.berkeley.cs.amplab.mlmatrix

import java.util.concurrent.ThreadLocalRandom

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import breeze.linalg._
import breeze.numerics._

class BlockQRSuite extends FunSuite with LocalSparkContext {

  test("Test R in QR") {
    sc = new SparkContext("local", "test")

    val numRows = 100
    val numCols = 100
    val rowsPerPart = 20
    val colsPerPart = 10

    val matrixRDD = sc.parallelize(1 to numRows).map { row =>
      Array.fill(numCols)(ThreadLocalRandom.current().nextGaussian())
    }
    val A = BlockPartitionedMatrix.fromArray(matrixRDD, rowsPerPart, colsPerPart)

    val localA = A.collect()
    val localR = qr.justR(localA)
  
    val r = new BlockQR().qrR(A).collect()
    assert(Utils.aboutEq(abs(r), abs(localR)))
  }
}
