package edu.berkeley.cs.amplab.mlmatrix

import java.util.concurrent.ThreadLocalRandom

import breeze.linalg._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.StatsReportListener

class NormalEquations extends Logging with Serializable {

  def solveLeastSquares(
      A: RowPartitionedMatrix,
      b: RowPartitionedMatrix) : DenseMatrix[Double]  = {
    val Ab = A.rdd.zip(b.rdd).map(x => (x._1.mat, x._2.mat))
    val ATb = Ab.map { part => 
      part._1.t * part._2
    }.reduce { case (a, b) => 
      a + b
    }

    val ATA = A.rdd.map { part => 
      part.mat.t * part.mat 
    }.reduce { case (a, b) => 
      a + b 
    }

    ATA \ ATb
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

    val rowsPerPart = numRows / numParts
    val matrixParts = sc.parallelize(1 to numParts, numParts).mapPartitions { part =>
      val data = new Array[Double](rowsPerPart * (numCols))
      var i = 0
      while (i < rowsPerPart * (numCols)) {
        data(i) = ThreadLocalRandom.current().nextGaussian()
        i = i + 1
      }
      val mat = new DenseMatrix[Double](rowsPerPart, numCols + 1, data)
      Iterator(mat)
    }
    matrixParts.cache().count()

    val A = RowPartitionedMatrix.fromMatrix(matrixParts)

    val b =  A.mapPartitions(
      part => DenseMatrix.rand(part.rows, numClasses)).cache()

    var begin = System.nanoTime()
    val x = new NormalEquations().solveLeastSquares(A, b)
    var end = System.nanoTime()
  }
}
