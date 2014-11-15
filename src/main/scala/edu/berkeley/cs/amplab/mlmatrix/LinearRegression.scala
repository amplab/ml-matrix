package edu.berkeley.cs.amplab.mlmatrix


import breeze.linalg._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkException
import org.apache.spark.scheduler.StatsReportListener

import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

/*Solves for x in formula Ax=b by minimizing the least squares loss function
 *using stochastic gradient descent*/

class LinearRegression(numIterations: Int, stepSize: Double) extends Logging with Serializable {

  def solveLeastSquares(A: RowPartitionedMatrix, b: RowPartitionedMatrix):
    RowPartitionedMatrix = {
    // map RDD[RowPartition] to RDD[LabeledPoint]
    val data = A.rdd.zip(b.rdd).flatMap{ x =>
      //var features = x._1.mat.toArray2().map(row => Vectors.dense(row))
      //  val rows = rowPart.mat.data.grouped(rowPart.mat.rows).toSeq.transpose
      val feature_rows = x._1.mat.data.grouped(x._1.mat.rows).toSeq.transpose
      var features = feature_rows.map(row => Vectors.dense(row.toArray))
      //val labels = x._2.mat.toArray2().map(row => row(0))
      val label_rows = x._2.mat.data.grouped(x._2.mat.rows).toSeq.transpose
      val labels = label_rows.map(row => row(0))

      features.zip(labels).map(x => LabeledPoint(x._2, x._1))
    }

    // Train(RDD[LabeledPoint], numIterations, stepSize, miniBatchFraction)
    val model = LinearRegressionWithSGD.train(data, numIterations, stepSize, 1.0)
    val x = model.weights // Model.weights is a Vector
    RowPartitionedMatrix.fromArray(A.rdd.context.parallelize(x.toArray.map(x => Array(x)),
      A.rdd.partitions.size))
  }
}

object LinearRegression extends Logging {

  def main(args: Array[String]) {
    if (args.length < 8) {
      println("Usage: LinearRegression <master> <sparkHome> <numRows> <numCols> <numParts> <numClasses> <numIterations> <stepSize> [cores]")
      System.exit(0)
    }
    val sparkMaster = args(0)
    val sparkHome = args(1)
    val numRows = args(2).toInt
    val numCols = args(3).toInt
    val numParts = args(4).toInt
    val numClasses = args(5).toInt
    val numIterations = args(6).toInt
    val stepSize = args(7).toDouble

    val coresPerTask = if (args.length > 8) {
      args(8).toInt
    } else {
      1
    }

    println("Num rows is " + numRows)
    println("Num cols is " + numCols)
    println("Num parts is " + numParts)
    println("Num classes is " + numClasses)
    println("Num iterations is " + numIterations)
    println("Step size is " + stepSize)

    val sc = new SparkContext(sparkMaster, "LinearRegression", sparkHome,
      SparkContext.jarOfClass(this.getClass).toSeq)
      sc.addSparkListener(new StatsReportListener)
      sc.setLocalProperty("spark.task.cpus", coresPerTask.toString)

    //val lss = LinearSystem.createLinearSystems(sc, numRows, numCols, numClasses,
    //  numParts, Seq(1.0), Seq(0.0))
    // val A = lss(0).A
    // val b = lss(0).b

    val A = RowPartitionedMatrix.createRandom(sc, numRows, numCols, numParts)
    val b =  A.mapPartitions(
      part => DenseMatrix.rand(part.rows, numClasses)).cache()
    // val b = A.mapPartitions { part =>
    //   val vec = new DoubleMatrix(2, 1, Array(5.0, 3.0):_*)
    //   part.mmul(vec)
    // }.cache()

    // val Ab = StabilityChecker.createVandermondeMatrix(sc, numRows, numCols, numParts)
    // val A = Ab._1
    // val b = Ab._2

    // val localA = A.collect()
    // val localB = b.collect()
    // val localX = Solve.solveLeastSquares(localA, localB)
    // val svds = Singular.SVDValues(localA)
    // val conditionNumber = svds.data.max / svds.data.min

    var begin = System.nanoTime()
    val x = new LinearRegression(numIterations, stepSize).solveLeastSquares(A, b).collect()
    var end = System.nanoTime()

    //println("For " + numRows + " " + localA.columns + " err " +
    //        StabilityChecker.computeRelativeError(x, localX) + " " + conditionNumber)
    //println("x " + x)
    //println("localX " + localX)

    logInfo("Linear solver of " + numRows + "x" + numCols + " took " + (end - begin)/1e6 + "ms")
  }
}
