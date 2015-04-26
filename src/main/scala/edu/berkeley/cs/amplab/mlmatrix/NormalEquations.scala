package edu.berkeley.cs.amplab.mlmatrix

import java.util.concurrent.ThreadLocalRandom

import breeze.linalg._

import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext

class NormalEquations extends RowPartitionedSolver with Logging with Serializable {

  def solveManyLeastSquaresWithL2(
      A: RowPartitionedMatrix,
      b: RowPartitionedMatrix,
      residuals: RDD[Array[DenseMatrix[Double]]],
      lambdas: Array[Double]): Seq[DenseMatrix[Double]] = {

    val Abs = A.rdd.zip(b.rdd).map { x =>
      (x._1.mat, x._2.mat)
    }

    val ATA_ATb = Abs.zip(residuals).map { part =>
      val aPart = part._1._1
      val bPart = part._1._2
      val res = part._2

      val AtA = aPart.t * aPart
      val AtBs = new Array[DenseMatrix[Double]](part._2.length)
      var i = 0
      val tmp = new DenseMatrix[Double](bPart.rows, bPart.cols)
      while (i < res.length) {
        tmp :+= bPart
        tmp :-= res(i)
        val atb = aPart.t * tmp
        AtBs(i) = atb
        java.util.Arrays.fill(tmp.data, 0.0)
        i = i + 1
      }
      (AtA, AtBs)
    }

    // val ATA_ATb = Abs.map { part =>
    //   val AtBs = part._2.map { b =>
    //     part._1.t * b
    //   }
    //   (AtA, AtBs)
    // }

    val treeBranchingFactor = A.rdd.context.getConf.getInt("spark.mlmatrix.treeBranchingFactor", 2).toInt
    val depth = math.ceil(math.log(ATA_ATb.partitions.size)/math.log(treeBranchingFactor)).toInt
    val reduced = Utils.treeReduce(ATA_ATb, reduceNormalMany, depth=depth)

    val ATA = reduced._1

    // Local solve
    val xs = lambdas.zip(reduced._2).map { l =>
      val gamma = DenseMatrix.eye[Double](ATA.rows)
      gamma :*= l._1
      (ATA + gamma) \ l._2
    }

    xs
  }

  private def reduceNormalMany(
    a: (DenseMatrix[Double], Array[DenseMatrix[Double]]),
    b: (DenseMatrix[Double], Array[DenseMatrix[Double]])):
      (DenseMatrix[Double], Array[DenseMatrix[Double]]) = {
    a._1 :+= b._1
    var i = 0
    while (i < a._2.length) {
      a._2(i) :+= b._2(i)
      i = i + 1
    }
    a
  }

  def solveLeastSquaresWithManyL2(
      A: RowPartitionedMatrix,
      b: RowPartitionedMatrix,
      lambdas: Array[Double]) : Seq[DenseMatrix[Double]]  = {
    val Ab = A.rdd.zip(b.rdd).map(x => (x._1.mat, x._2.mat))
    val ATA_ATb = Ab.map { part =>
      (part._1.t * part._1, part._1.t * part._2)
    }

    val treeBranchingFactor = A.rdd.context.getConf.getInt("spark.mlmatrix.treeBranchingFactor", 2).toInt
    val depth = math.ceil(math.log(ATA_ATb.partitions.size)/math.log(treeBranchingFactor)).toInt
    val reduced = Utils.treeReduce(ATA_ATb, reduceNormal, depth=depth)

    val xs = lambdas.map { l =>
      val gamma = DenseMatrix.eye[Double](reduced._1.rows)
      gamma :*= l
      (reduced._1 + gamma) \ reduced._2
    }

    xs
  }

  private def reduceNormal(
    a: (DenseMatrix[Double], DenseMatrix[Double]),
    b: (DenseMatrix[Double], DenseMatrix[Double])): (DenseMatrix[Double], DenseMatrix[Double]) = {
    a._1 :+= b._1
    a._2 :+= b._2
    a
  }

}

object NormalEquations {

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

    sc.stop()
    println("Normal equations took " + (end-begin)/1e6 + " ms")
  }
}
