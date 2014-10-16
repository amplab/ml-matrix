package edu.berkeley.cs.amplab.mlmatrix

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import breeze.linalg._
import breeze.numerics._

class BlockCoordinateDescentSuite extends FunSuite with LocalSparkContext {

  def horzCatReduce(a: DenseMatrix[Double], b: DenseMatrix[Double]) = {
    DenseMatrix.horzcat(a, b)
  }

  def vertCatReduce(a: DenseMatrix[Double], b: DenseMatrix[Double]) = {
    DenseMatrix.vertcat(a, b)
  }

  test("test Seq[RowPartitionedMatrix] using NormalEquations") {
    sc = new SparkContext("local", "test")
    val nParts = 2

    val aParts = (0 until nParts).map { p =>
      RowPartitionedMatrix.createRandom(sc, 4096, 16, 4, cache=true)
    }

    val b =  aParts(0).mapPartitions(
      part => DenseMatrix.rand(part.rows, 1)).cache()

    val localAs: Seq[DenseMatrix[Double]] = aParts.map(p => p.collect())
    val localFullA = localAs.reduceLeft(horzCatReduce)
    val localb = b.collect()

    val localXFull = localFullA \ localb

    val xs = BlockCoordinateDescent.solveLeastSquaresWithL2(aParts, b, Array(0.0), 10,
      new NormalEquations()).map(x => x.head)

    val xFull = xs.reduceLeft(vertCatReduce)

    val diff = abs(xFull - localXFull)
    println("max diff " + max(diff))
    println("Diff norm2 " + norm(diff.toDenseVector))
    println("Actual norm2 " + norm(localXFull.toDenseVector))

    println("BCD Residual " + norm((localFullA * xFull - localb).toDenseVector))
    println("Actual Residual " + norm((localFullA * localXFull - localb).toDenseVector))
    assert(Utils.aboutEq(xFull, localXFull, 1e-1))
  }

  test("test Seq[RowPartitionedMatrix] using TSQR") {
    sc = new SparkContext("local", "test")
    val nParts = 2

    val aParts = (0 until nParts).map { p =>
      RowPartitionedMatrix.createRandom(sc, 4096, 16, 4, cache=true)
    }

    val b =  aParts(0).mapPartitions(
      part => DenseMatrix.rand(part.rows, 1)).cache()

    val localAs: Seq[DenseMatrix[Double]] = aParts.map(p => p.collect())
    val localFullA = localAs.reduceLeft(horzCatReduce)
    val localb = b.collect()

    val localXFull = localFullA \ localb

    val xs = BlockCoordinateDescent.solveLeastSquaresWithL2(aParts, b, Array(0.0), 10,
      new TSQR()).map(x => x.head)

    val xFull = xs.reduceLeft(vertCatReduce)

    val diff = abs(xFull - localXFull)
    println("max diff " + max(diff))
    println("Diff norm2 " + norm(diff.toDenseVector))
    println("Actual norm2 " + norm(localXFull.toDenseVector))

    println("BCD Residual " + norm((localFullA * xFull - localb).toDenseVector))
    println("Actual Residual " + norm((localFullA * localXFull - localb).toDenseVector))
    assert(Utils.aboutEq(xFull, localXFull, 1e-1))
  }

  test("test one pass block coordinate descent") {
    sc = new SparkContext("local", "test")
    val nParts = 8

    val aParts = (0 until nParts).map { p =>
      RowPartitionedMatrix.createRandom(sc, 4096, 16, 4, cache=true)
    }

    val b =  aParts(0).mapPartitions(
      part => DenseMatrix.rand(part.rows, 1)).cache()

    val localAs: Seq[DenseMatrix[Double]] = aParts.map(p => p.collect())
    val localb = b.collect()

    val xsIter = BlockCoordinateDescent.solveOnePassL2(aParts.iterator, b, Array(0.0),
      new NormalEquations()).map(x => x.head)

    var curResidual = new DenseMatrix[Double](4096, 1)

    val residuals = xsIter.zip(localAs.iterator).map { case (xs, aPart) =>
      val thisResidual = aPart * xs
      curResidual :+= thisResidual
      norm((curResidual - localb).toDenseVector)
    }.toSeq

    var prevResidual = residuals(0)
    var i = 0
    while (i < residuals.length) {
      assert(residuals(i) <= prevResidual)
      prevResidual = residuals(i)
      i = i + 1
    }

    println("Residuals: ")
    residuals.foreach { r =>
      printf("%f ", r)
    }
    println()
  }

}
