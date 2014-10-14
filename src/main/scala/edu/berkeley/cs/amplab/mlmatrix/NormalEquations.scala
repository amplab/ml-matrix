package edu.berkeley.cs.amplab.mlmatrix

import java.util.concurrent.ThreadLocalRandom

import breeze.linalg._

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext

import edu.berkeley.cs.amplab.mlmatrix.util.Utils

class NormalEquations extends Logging with Serializable {

  def solveLeastSquares(
      A: RowPartitionedMatrix,
      b: RowPartitionedMatrix) : DenseMatrix[Double]  = {
    val Ab = A.rdd.zip(b.rdd).map(x => (x._1.mat, x._2.mat))
    val ATA_ATb = Ab.map { part =>
      (part._1.t * part._1, part._1.t * part._2)
    }

    val reduced = Utils.treeReduce(ATA_ATb, reduceNormal, depth=2)
    reduced._1 \ reduced._2
  }

  private def reduceNormal(
    a: (DenseMatrix[Double], DenseMatrix[Double]),
    b: (DenseMatrix[Double], DenseMatrix[Double])): (DenseMatrix[Double], DenseMatrix[Double]) = {
    a._1 :+= b._1
    a._2 :+= b._2
    a
  }
}

object NormalEquations extends Logging {

  def main(args: Array[String]) {
    if (args.length < 5) {
      println("Usage: NormalEquations <master> <numRows> <numCols> <numParts> <numClasses>")
      System.exit(0)
    }

    val sparkMaster = args(0)
    val numRows = args(1).toInt
    val numCols = args(2).toInt
    val numParts = args(3).toInt
    val numClasses = args(4).toInt

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("NormalEquations")
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    val sc = new SparkContext(conf)

    val A = RowPartitionedMatrix.createRandom(sc, numRows, numCols, numParts, cache=true)

    val b =  A.mapPartitions(
      part => DenseMatrix.rand(part.rows, numClasses)).cache()

    var begin = System.nanoTime()
    val x = new NormalEquations().solveLeastSquares(A, b)
    var end = System.nanoTime()

    println("Normal equations took " + (end-begin)/1e6 + " ms")
  }
}
