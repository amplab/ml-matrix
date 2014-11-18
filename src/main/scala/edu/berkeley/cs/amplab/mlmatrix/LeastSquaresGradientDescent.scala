package edu.berkeley.cs.amplab.mlmatrix

import breeze.linalg._

import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.scheduler.StatsReportListener

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

 /**
  *Solves for x in formula Ax=b by minimizing the least squares loss function
  *using stochastic gradient descent
  */
class LeastSquaresGradientDescent(numIterations: Int, stepSize: Double,
  miniBatchFraction: Double) extends RowPartitionedSolver with Logging with Serializable {

  override def solveLeastSquares(A: RowPartitionedMatrix, b: RowPartitionedMatrix):
    DenseMatrix[Double] = {

    if(b.getDim()._2 != 1) {
      throw new SparkException(
        "Multiple right hand sides are not supported")
    }

    // map RDD[RowPartition] to RDD[LabeledPoint]
    val data = A.rdd.zip(b.rdd).flatMap { x =>
      val feature_rows = x._1.mat.data.grouped(x._1.mat.rows).toSeq.transpose
      var features = feature_rows.map(row => Vectors.dense(row.toArray))
      val label_rows = x._2.mat.data.grouped(x._2.mat.rows).toSeq.transpose
      val labels = label_rows.map(row => row(0))
      features.zip(labels).map(x => LabeledPoint(x._2, x._1))
    }

    // Train(RDD[LabeledPoint], numIterations, stepSize, miniBatchFraction)
    val model = LinearRegressionWithSGD.train(data, numIterations, stepSize, miniBatchFraction)
    DenseMatrix(model.weights.toArray)
  }

  def solveLeastSquaresWithManyL2(
      A: RowPartitionedMatrix,
      b: RowPartitionedMatrix,
      lambdas: Array[Double]): Seq[DenseMatrix[Double]] = {

    if(b.getDim()._2 != 1) {
      throw new SparkException(
        "Multiple right hand sides are not supported")
    }

    lambdas.map { lambda =>
      val data = A.rdd.zip(b.rdd).flatMap { x =>
        val feature_rows = x._1.mat.data.grouped(x._1.mat.rows).toSeq.transpose
        var features = feature_rows.map(row => Vectors.dense(row.toArray))
        val label_rows = x._2.mat.data.grouped(x._2.mat.rows).toSeq.transpose
        val labels = label_rows.map(row => row(0))

        features.zip(labels).map(x => LabeledPoint(x._2, x._1))
      }

      val model = RidgeRegressionWithSGD.train(data, numIterations, stepSize,
        lambda, miniBatchFraction)
      DenseMatrix(model.weights.toArray)
    }
  }

  def solveManyLeastSquaresWithL2(
      A: RowPartitionedMatrix,
      b: RDD[Seq[DenseMatrix[Double]]],
      lambdas: Array[Double]): Seq[DenseMatrix[Double]] = {

    val multipleRHS = b.map { matSeq =>
      matSeq.forall{ mat => mat.cols!=1}
    }.reduce{ (a,b) => a & b}

    if (multipleRHS) {
      throw new SparkException(
        "Multiple right hand sides are not supported"
      )
    }
    val lambdaWithIndex = lambdas.zipWithIndex

    lambdaWithIndex.map { lambdaI =>

      val lambda = lambdaI._1
      val bIndex = lambdaI._2

      // b is an RDD
      val data = A.rdd.zip(b).flatMap { x =>
        val bVector = x._2(bIndex)
        // TODO: Write a util function to convert breeze matrix to Array[Vector]
        val feature_rows = x._1.mat.data.grouped(x._1.mat.rows).toSeq.transpose
        var features = feature_rows.map(row => Vectors.dense(row.toArray))
        val label_rows = bVector.data.grouped(bVector.rows).toSeq.transpose
        val labels = label_rows.map(row => row(0))
        features.zip(labels).map(x => LabeledPoint(x._2, x._1))
      }

      val model = RidgeRegressionWithSGD.train(data, numIterations, stepSize,
        lambda, miniBatchFraction)
        DenseMatrix(model.weights.toArray)
    }
  }
}

object LeastSquaresGradientDescent extends Logging {

  def main(args: Array[String]) {
    if (args.length < 8) {
      println("Usage: LeastSquaresGradientDescent  <master> <numRows> <numCols> <numParts> <numClasses> <numIterations> <stepSize> <miniBatchFraction")
      System.exit(0)
    }
    val sparkMaster = args(0)
    val numRows = args(1).toInt
    val numCols = args(2).toInt
    val numParts = args(3).toInt
    val numClasses = args(4).toInt
    val numIterations = args(5).toInt
    val stepSize = args(6).toDouble
    val miniBatchFraction = args(7).toDouble

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("LeastSquaresGradientDescent")
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      val sc = new SparkContext(conf)


    val A = RowPartitionedMatrix.createRandom(sc, numRows, numCols, numParts)
    val b =  A.mapPartitions(
      part => DenseMatrix.rand(part.rows, numClasses)).cache()

    var begin = System.nanoTime()
    val x = new LeastSquaresGradientDescent(numIterations, stepSize, 1.0).solveLeastSquares(A, b)
    var end = System.nanoTime()

    logInfo("Linear solver of " + numRows + "x" + numCols + " took " + (end - begin)/1e6 + "ms")
  }
}
