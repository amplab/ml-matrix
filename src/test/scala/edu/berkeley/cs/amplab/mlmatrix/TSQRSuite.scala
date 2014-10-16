package edu.berkeley.cs.amplab.mlmatrix

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import breeze.linalg._
import breeze.numerics._

class TSQRSuite extends FunSuite with LocalSparkContext {

  test("Test R in QR") {
    sc = new SparkContext("local", "test")
    val A = RowPartitionedMatrix.createRandom(sc, 128, 16, 4, cache=true)
    val localA = A.collect()
  
    val r = new TSQR().qrR(A)
    val localR = qr.justR(localA)
  
    assert(Utils.aboutEq(abs(r), abs(localR)))
  }

  test("Test Q in QR") {
    sc = new SparkContext("local", "test")
    val A = RowPartitionedMatrix.createRandom(sc, 8, 4, 2, cache=true)
    val localA = A.collect()

    val (q, r) = new TSQR().qrQR(A)
    val localQR = qr(localA)

    assert(Utils.aboutEq(abs(r), abs(localQR.r)))

    val qComputed = q.collect()
    assert(Utils.aboutEq(abs(q.collect()), abs(localQR.q)))
  }

  test("Test QR solver") {
    sc = new SparkContext("local", "test")
    val A = RowPartitionedMatrix.createRandom(sc, 128, 16, 4, cache=true)
    val b =  A.mapPartitions(
      part => DenseMatrix.rand(part.rows, 1)).cache()
  
    val localA = A.collect()
    val localB = b.collect()
  
    val x = new TSQR().solveLeastSquares(A, b)
  
    val localX = localA \ localB
  
    assert(Utils.aboutEq(x, localX))
  }

  test("Test QR solver with regularization") {
    sc = new SparkContext("local", "test")
    val A = RowPartitionedMatrix.createRandom(sc, 128, 16, 4, cache=true)
    val b =  A.mapPartitions(
      part => DenseMatrix.rand(part.rows, 1)).cache()
  
    val localA = A.collect()
    val localB = b.collect()

    val lambdas = Array(0.0, 1e-4, 1e-6, 100)
  
    val xs = new TSQR().solveLeastSquaresWithManyL2(A, b, lambdas)

    for (i <- 0 until lambdas.length) {
      val x = xs(i)
      val reg = DenseMatrix.eye[Double](16) :* lambdas(i)
      // TODO: This checks our solution with the same method applied locally
      // Investigate why using breeze's default solver gives some numerical error.
      val toSolve = DenseMatrix.vertcat(localA, reg)
      val localQR = qr(toSolve)
      val localX = localQR.r \ (localQR.q.t * DenseMatrix.vertcat(localB,
        DenseMatrix.zeros[Double](16, 1)))
      assert(Utils.aboutEq(x, localX))
    }
  }

}
