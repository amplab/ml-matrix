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
      b: RDD[Seq[DenseMatrix[Double]]],
      lambdas: Array[Double]): Seq[DenseMatrix[Double]] = {

    val Abs = A.rdd.zip(b).map { x =>
      (x._1.mat, x._2)
    }

    val ATA_ATb = Abs.map { part =>
      val AtA = part._1.t * part._1
      val AtBs = part._2.map { b =>
        part._1.t * b
      }
      (AtA, AtBs)
    }

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
    a: (DenseMatrix[Double], Seq[DenseMatrix[Double]]),
    b: (DenseMatrix[Double], Seq[DenseMatrix[Double]])):
      (DenseMatrix[Double], Seq[DenseMatrix[Double]]) = {
    a._1 :+= b._1
    a._2.zip(b._2).map { z =>
      z._1 :+= z._2
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
