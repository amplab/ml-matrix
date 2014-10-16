package edu.berkeley.cs.amplab.mlmatrix

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import breeze.linalg._

class NormalEquationsSuite extends FunSuite with LocalSparkContext {

  test("Test NormalEquations") {
    sc = new SparkContext("local", "test")
    val A = RowPartitionedMatrix.createRandom(sc, 128, 16, 4, cache=true)
    val b =  A.mapPartitions(
      part => DenseMatrix.rand(part.rows, 1)).cache()

    val localA = A.collect()
    val localB = b.collect()

    val x = new NormalEquations().solveLeastSquares(A, b)
    val localX = localA \ localB

    assert(Utils.aboutEq(x, localX))
  }

}
