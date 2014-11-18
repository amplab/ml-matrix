package edu.berkeley.cs.amplab.mlmatrix

import java.util.concurrent.ThreadLocalRandom

import breeze.linalg._

import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkException
import org.apache.spark.scheduler.StatsReportListener

object StabilityChecker extends Logging {

  def computeRelativeError(xComputed: DenseMatrix[Double], xLocal: DenseMatrix[Double]) = {
    val relativeError = norm((xComputed - xLocal).toDenseVector)/norm(xLocal.toDenseVector)
    relativeError
  }

  def main(args: Array[String]) {
    if (args.length < 8) {
      println("Usage: StabilityChecker <master> <sparkHome> <numRows> <numCols> <numParts> " +
        "<numClasses> <solver: tsqr|normal|sgd|local> <stepsize> <numIters> <miniBatchFraction>")
      System.exit(0)
    }

    val sparkMaster = args(0)
    val sparkHome = args(1)
    val numRows = args(2).toInt
    val numCols = args(3).toInt
    val numParts = args(4).toInt
    val numClasses = args(5).toInt
    val solver = args(6)

    val coresPerTask = 1
    val stepSize = args(7).toDouble
    val numIterations = args(8).toInt
    val miniBatchFraction = args(9).toDouble

    val sc = new SparkContext(sparkMaster, "NormalEquations", sparkHome,
      SparkContext.jarOfClass(this.getClass).toSeq)
    sc.addSparkListener(new StatsReportListener)
    sc.setLocalProperty("spark.task.cpus", coresPerTask.toString)

    //val thetas = Seq(0, 0.25, 0.5, 0.75, 1, 1.25, 1.5)
    val thetas = Seq(3.74e-6)
    //val condNumbers = Seq(2.27e10)
    val condNumbers = Seq(1.0, 10, 1e3, 1e6, 1e9, 1e12, 1e15)

    val lss = LinearSystem.createLinearSystems(sc, numRows, numCols, numClasses,
      numParts, condNumbers, thetas)
    // val colRange = Seq(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
    // val lss = colRange.map(c => createVandermondeMatrix(sc, numRows, c, numParts))
    var i=0
    lss.foreach { ls =>
      // Verify this locally
      val localA = ls.A.collect()
      val xLocal = localA \ ls.b.collect()
      val svd.SVD(u,s,v) = svd(localA)
      val conditionNumber = s.data.max / s.data.min
      var begin = System.nanoTime()
      val xComputed = solver.toLowerCase match {
        case "normal" =>
          new NormalEquations().solveLeastSquares(ls.A, ls.b)
        case "sgd" =>
          new LeastSquaresGradientDescent(numIterations, stepSize, 1.0).solveLeastSquares(ls.A, ls.b)
        case "tsqr" =>
          new TSQR().solveLeastSquares(ls.A, ls.b)
        case "local" =>
          val (r, qtb) = QRUtils.qrSolve(ls.A.collect(), ls.b.collect())
          //val svds = Singular.SVDValues(ls.A.collect())
          //println("Singluar values " + svds.data.mkString(","))
          r \ qtb
        case _ =>
          logError("Invalid Solver " + solver + " should be one of tsqr|normal|sgd")
          logError("USING TSQR")
          new TSQR().solveLeastSquares(ls.A, ls.b)
      }
      var end = System.nanoTime()
      //println("For " + numRows + " " + localA.columns + " err " +
      //        computeRelativeError(xComputed, xLocal) + " " + conditionNumber)
      ls.computeResidualNorm(xComputed)
      ls.computeRelativeError(xComputed)
      println("solver: " + solver + " of " + numRows + " x " + localA.cols + " took " +
        (end - begin)/1e6 + "ms")

      val R = ls.A.qrR()
      val svd.SVD(uR,sR,vR) = svd(localA)
      val conditionNumberR = sR.data.max / sR.data.min


      //println("Condition number of A inputed by algorithm is " + condNumbers(i))
      //println("Condition number of A computed by SVD is " + conditionNumber)
      println(conditionNumberR + ", " + ls.A.condEst(R))
      i = i+1
    }
  }
}
