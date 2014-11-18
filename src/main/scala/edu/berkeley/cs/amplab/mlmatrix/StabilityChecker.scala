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

  def main(args: Array[String]) {
    if (args.length < 6) {
      println("Usage: StabilityChecker <master> <sparkHome> <numRows> <numCols> <numParts> " +
        "<numClasses> <solver: tsqr|normal|sgd|local> [<stepsize> <numIters> <miniBatchFraction>]")
      System.exit(0)
    }

    val sparkMaster = args(0)
    val numRows = args(1).toInt
    val numCols = args(2).toInt
    val numParts = args(3).toInt
    val numClasses = args(4).toInt
    val solver = args(5)

    var stepSize = 0.1
    var numIterations = 10
    var miniBatchFraction = 1.0

    if (solver == "sgd") { 
      if (args.length < 9) {
        println("Usage: StabilityChecker <master> <sparkHome> <numRows> <numCols> <numParts> " +
          "<numClasses> <solver: tsqr|normal|sgd|local> [<stepsize> <numIters> <miniBatchFraction>]")
        System.exit(0)
      } else {
        stepSize = args(6).toDouble
        numIterations = args(7).toInt
        miniBatchFraction = args(8).toDouble
      }
    }

    val sc = new SparkContext(sparkMaster, "StabilityChecker",
      jars=SparkContext.jarOfClass(this.getClass).toSeq)
    sc.addSparkListener(new StatsReportListener)

    val thetas = Seq(3.74e-6)
    val condNumbers = Seq(1.0, 10, 1e3, 1e6, 1e9, 1e12, 1e15)

    val lss = LinearSystem.createLinearSystems(sc, numRows, numCols, numClasses,
      numParts, condNumbers, thetas)

    lss.foreach { ls =>
      var begin = System.nanoTime()
      val xComputed = solver.toLowerCase match {
        case "normal" =>
          new NormalEquations().solveLeastSquares(ls.A, ls.b)
        case "sgd" =>
          new LeastSquaresGradientDescent(numIterations, stepSize, miniBatchFraction).solveLeastSquares(ls.A, ls.b)
        case "tsqr" =>
          new TSQR().solveLeastSquares(ls.A, ls.b)
        case "local" =>
          val (r, qtb) = QRUtils.qrSolve(ls.A.collect(), ls.b.collect())
          r \ qtb
        case _ =>
          logError("Invalid Solver " + solver + " should be one of tsqr|normal|sgd")
          logError("Using TSQR")
          new TSQR().solveLeastSquares(ls.A, ls.b)
      }
      var end = System.nanoTime()
      ls.computeResidualNorm(xComputed)
      ls.computeRelativeError(xComputed)
      logInfo("Solver: " + solver + " of " + numRows + " x " + ls.A.numCols + " took " +
        (end - begin)/1e6 + "ms")

      val R = ls.A.qrR()
      val svd.SVD(uR,sR,vR) = svd(R)
      val conditionNumberR = sR.data.max / sR.data.min

      logInfo("Actual condition number " + conditionNumberR + ", Estimate: " + ls.A.condEst(Some(R)))
    }
  }
}
