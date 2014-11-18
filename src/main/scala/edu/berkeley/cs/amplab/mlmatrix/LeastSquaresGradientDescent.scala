package edu.berkeley.cs.amplab.mlmatrix


import breeze.linalg._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkException
import org.apache.spark.scheduler.StatsReportListener

import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

/*Solves for x in formula Ax=b by minimizing the least squares loss function
 *using stochastic gradient descent*/

class LeastSquaresGradientDescent(numIterations: Int, stepSize: Double,
  miniBatchFraction: Double) extends RowPartitionedSolver with Logging with Serializable {

  override def solveLeastSquares(A: RowPartitionedMatrix, b: RowPartitionedMatrix):
    DenseMatrix[Double] = {

    if(b.getDim()._2 != 1) {
      throw new SparkException(
        "Multiple right hand sides are not supported")
    }

    /*
    val data = A.rdd.zip(b.rdd).flatMap{ x =>
      var features = x._1.mat.toArray2().map(row => Vectors.dense(row))
      val labels = x._2.mat.toArray2().map(row => row(0))
      features.zip(labels).map(x => LabeledPoint(x._2, x._1))
    }
    */

    // map RDD[RowPartition] to RDD[LabeledPoint]
    val data = A.rdd.zip(b.rdd).flatMap{ x =>
      //var features = x._1.mat.toArray2().map(row => Vectors.dense(row))

      val feature_rows = x._1.mat.data.grouped(x._1.mat.rows).toSeq.transpose
      var features = feature_rows.map(row => Vectors.dense(row.toArray))
      //val labels = x._2.mat.toArray2().map(row => row(0))
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

    lambdas.map{ lambda =>
      val data = A.rdd.zip(b.rdd).flatMap{ x =>
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

    val multipleRHS = b.map{ matSeq =>
      matSeq.forall{ mat => mat.cols!=1}
    }.reduce{ (a,b) => a & b}

    if (multipleRHS) {
      throw new SparkException(
        "Multiple right hand sides are not supported"
      )
    }
    val lambdaWithIndex = lambdas.zipWithIndex

    lambdaWithIndex.map{ lambdaI =>
      //b is an RDD
      val lambda = lambdaI._1
      val bIndex = lambdaI._2

      val data = A.rdd.zip(b).flatMap{ x =>
        //index into b.rdd ( Sequence of DenseMatrices with the same
        //index given by lambdas

        val bVector = x._2(bIndex)

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
      println("Usage: LeastSquaresGradientDescent  <master> <sparkHome> <numRows> <numCols> <numParts> <numClasses> <numIterations> <stepSize> [cores]")
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

    val sc = new SparkContext(sparkMaster, "LeastSquaresGradientDescent", sparkHome,
      SparkContext.jarOfClass(this.getClass).toSeq)
      sc.setLocalProperty("spark.task.cpus", coresPerTask.toString)


    val A = RowPartitionedMatrix.createRandom(sc, numRows, numCols, numParts)
    val b =  A.mapPartitions(
      part => DenseMatrix.rand(part.rows, numClasses)).cache()

    var begin = System.nanoTime()
    val x = new LeastSquaresGradientDescent(numIterations, stepSize, 1.0).solveLeastSquares(A, b)
    var end = System.nanoTime()

    logInfo("Linear solver of " + numRows + "x" + numCols + " took " + (end - begin)/1e6 + "ms")
  }
}
