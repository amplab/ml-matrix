package edu.berkeley.cs.amplab.mlmatrix

import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ArrayBuffer

import breeze.linalg._

import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Accumulator
import org.apache.spark.SparkContext._

class TSQR extends RowPartitionedSolver with Logging with Serializable {

  def qrR(mat: RowPartitionedMatrix): DenseMatrix[Double] = {
    val localQR = mat.rdd.context.accumulator(0.0, "Time taken for Local QR")

    val qrTree = mat.rdd.map { part =>
      if (part.mat.rows < part.mat.cols) {
        part.mat
      } else {
        val begin = System.nanoTime
        val r = QRUtils.qrR(part.mat)
        localQR += ((System.nanoTime - begin) / 1000000)
        r
      }
    }
    val depth = math.ceil(math.log(mat.rdd.partitions.size)/math.log(2)).toInt
    Utils.treeReduce(qrTree, reduceQR(localQR, _ : DenseMatrix[Double], _ : DenseMatrix[Double]), depth=depth)
  }

  private def reduceQR(acc: Accumulator[Double], a: DenseMatrix[Double], b: DenseMatrix[Double]): DenseMatrix[Double] = {
    val begin = System.nanoTime
    val out = QRUtils.qrR(DenseMatrix.vertcat(a, b), false)
    acc += ((System.nanoTime - begin) / 1e6)
    out
  }

  def qrQR(mat: RowPartitionedMatrix): (RowPartitionedMatrix, DenseMatrix[Double]) = {
    // First step run TSQR, get YTR tree
    val (qrTree, r) = qrYTR(mat)

    var curTreeIdx = qrTree.size - 1

    // Now construct Q by going up the tree
    var qrRevTree = qrTree(curTreeIdx)._2.map { part =>
      val yPart = part._2._1
      val tPart = part._2._2
      val qIn = new DenseMatrix[Double](yPart.rows, yPart.cols)
      for (i <- 0 until yPart.cols) {
        qIn(i, i) =  1.0
      }
      (part._1, QRUtils.applyQ(yPart, tPart, qIn, transpose=false))
    }.flatMap { x =>
      val nrows = x._2.rows
      Iterator((x._1 * 2, x._2),
               (x._1 * 2 + 1, x._2))
    }

    var prevTree = qrRevTree

    while (curTreeIdx > 0) {
      curTreeIdx = curTreeIdx - 1
      prevTree = qrRevTree
      if (curTreeIdx > 0) {
        val nextNumParts = qrTree(curTreeIdx - 1)._1
        qrRevTree = qrTree(curTreeIdx)._2.join(prevTree).flatMap { part =>
          val yPart = part._2._1._1
          val tPart = part._2._1._2
          val qPart = if (part._1 % 2 == 0) {
            val e = math.min(yPart.rows, yPart.cols)
            part._2._2(0 until e, ::)
          } else {
            val numRows = math.min(yPart.rows, yPart.cols)
            val s = part._2._2.rows - numRows
            part._2._2(s until part._2._2.rows, ::)
          }
          if (part._1 * 2 + 1 < nextNumParts) {
            val qOut = QRUtils.applyQ(yPart, tPart, qPart, transpose=false)
            val nrows = qOut.rows
            Iterator((part._1 * 2, qOut),
                     (part._1 * 2 + 1, qOut))
          } else {
            Iterator((part._1 * 2, qPart))
          }
        }
      } else {
        qrRevTree = qrTree(curTreeIdx)._2.join(prevTree).map { part =>
          val yPart = part._2._1._1
          val tPart = part._2._1._2
          val qPart = if (part._1 % 2 == 0) {
            val e = math.min(yPart.rows, yPart.cols)
            part._2._2(0 until e, ::)
          } else {
            val numRows = math.min(yPart.rows, yPart.cols)
            val s = part._2._2.rows - numRows
            part._2._2(s until part._2._2.rows, ::)
          }
          (part._1, QRUtils.applyQ(yPart, tPart, qPart, transpose=false))
        }
      }
    }

    (RowPartitionedMatrix.fromMatrix(qrRevTree.map(x => x._2)), r)
  }

  private def qrYTR(mat: RowPartitionedMatrix):
      (Seq[(Int, RDD[(Int, (DenseMatrix[Double], Array[Double], DenseMatrix[Double]))])],
        DenseMatrix[Double]) = {
    val qrTreeSeq = new ArrayBuffer[(Int, RDD[(Int, (DenseMatrix[Double], Array[Double], DenseMatrix[Double]))])]

    val matPartInfo = mat.getPartitionInfo
    val matPartInfoBroadcast = mat.rdd.context.broadcast(matPartInfo)

    var qrTree = mat.rdd.mapPartitionsWithIndex { case (part, iter) =>
      if (matPartInfoBroadcast.value.contains(part) && !iter.isEmpty) {
        val partBlockIds = matPartInfoBroadcast.value(part).sortBy(x=> x.blockId).map(x => x.blockId)
        iter.zip(partBlockIds.iterator).map { case (lm, bi) =>
          if (lm.mat.rows < lm.mat.cols) {
            (
              bi,
              (new DenseMatrix[Double](lm.mat.rows, lm.mat.cols),
               new Array[Double](lm.mat.rows),
              lm.mat)
            )
          } else {
            val qrResult = QRUtils.qrYTR(lm.mat)
            (bi, qrResult)
          }
        }
      } else {
        Iterator()
      }
    }

    var numParts = matPartInfo.flatMap(x => x._2.map(y => y.blockId)).size
    qrTreeSeq.append((numParts, qrTree))

    while (numParts > 1) {
      qrTree = qrTree.map(x => ((x._1/2.0).toInt, x._2)).reduceByKey(
        numPartitions=math.ceil(numParts/2.0).toInt,
        func=reduceYTR(_, _))
      numParts = math.ceil(numParts/2.0).toInt
      qrTreeSeq.append((numParts, qrTree))
    }
    val r = qrTree.map(x => x._2._3).collect()(0)
    (qrTreeSeq, r)
  }

  private def reduceYTR(
      a: (DenseMatrix[Double], Array[Double], DenseMatrix[Double]),
      b: (DenseMatrix[Double], Array[Double], DenseMatrix[Double]))
    : (DenseMatrix[Double], Array[Double], DenseMatrix[Double]) = {
    QRUtils.qrYTR(DenseMatrix.vertcat(a._3, b._3))
  }

  // From http://math.stackexchange.com/questions/299481/qr-factorization-for-ridge-regression
  // To solve QR with L2, we need to factorize \pmatrix{ A \\ \Gamma}
  // i.e. A and \Gamma stacked vertically, where \Gamma is a nxn Matrix.
  // To do this we first use TSQR on A and then locally stack \Gamma below and recompute QR.
  def solveLeastSquaresWithManyL2(
      A: RowPartitionedMatrix,
      b: RowPartitionedMatrix,
      lambdas: Array[Double]): Seq[DenseMatrix[Double]] = {
    val matrixParts = A.rdd.zip(b.rdd).map(x => (x._1.mat, x._2.mat))
    val qrTree = matrixParts.map { part =>
      val (aPart, bPart) = part
      if (aPart.rows < aPart.cols) {
        (aPart, bPart)
      } else {
        QRUtils.qrSolve(aPart, bPart)
      }
    }

    val depth = math.ceil(math.log(A.rdd.partitions.size)/math.log(2)).toInt
    val qrResult = Utils.treeReduce(qrTree, reduceQRSolve, depth=depth)

    val results = lambdas.map { lambda =>
      // We only have one partition right now
      val (rFinal, bFinal) = qrResult
      val out = if (lambda == 0.0) {
        rFinal \ bFinal
      } else {
        val lambdaRB = (DenseMatrix.eye[Double](rFinal.cols) :* math.sqrt(lambda),
          new DenseMatrix[Double](rFinal.cols, bFinal.cols))
        val reduced = reduceQRSolve((rFinal, bFinal), lambdaRB)
        reduced._1 \ reduced._2
      }
      out
    }
    results
  }

  private def reduceQRSolve(
      a: (DenseMatrix[Double], DenseMatrix[Double]),
      b: (DenseMatrix[Double], DenseMatrix[Double])): (DenseMatrix[Double], DenseMatrix[Double]) = {
    QRUtils.qrSolve(DenseMatrix.vertcat(a._1, b._1),
      DenseMatrix.vertcat(a._2, b._2))
  }

  def solveManyLeastSquaresWithL2(
      A: RowPartitionedMatrix,
      b: RDD[Seq[DenseMatrix[Double]]],
      lambdas: Array[Double]): Seq[DenseMatrix[Double]] = {

    val matrixParts = A.rdd.zip(b).map { x =>
      (x._1.mat, x._2)
    }

    val qrTree = matrixParts.map { part =>
      val (aPart, bParts) = part

      if (aPart.rows < aPart.cols) {
        (aPart, bParts)
      } else {
        QRUtils.qrSolveMany(aPart, bParts)
      }
    }

    val qrResult = Utils.treeReduce(qrTree, reduceQRSolveMany, 
      depth=math.ceil(math.log(A.rdd.partitions.size)/math.log(2)).toInt)
    val rFinal = qrResult._1

    val results = lambdas.zip(qrResult._2).map { case (lambda, bFinal) =>
      // We only have one partition right now
      val out = if (lambda == 0.0) {
        rFinal \ bFinal
      } else {
        val lambdaRB = (DenseMatrix.eye[Double](rFinal.cols) :* math.sqrt(lambda),
          new DenseMatrix[Double](rFinal.cols, bFinal.cols))
        val reduced = reduceQRSolve((rFinal, bFinal), lambdaRB)
        reduced._1 \ reduced._2
      }
      out
    }
    results
  }

  private def reduceQRSolveMany(
      a: (DenseMatrix[Double], Seq[DenseMatrix[Double]]),
      b: (DenseMatrix[Double], Seq[DenseMatrix[Double]])):
        (DenseMatrix[Double], Seq[DenseMatrix[Double]]) = {
    QRUtils.qrSolveMany(DenseMatrix.vertcat(a._1, b._1),
      a._2.zip(b._2).map(x => DenseMatrix.vertcat(x._1, x._2)))
  }
}

object TSQR extends Logging {

  def main(args: Array[String]) {
    if (args.length < 5) {
      println("Usage: TSQR <master> <numRows> <numCols> <numParts> <numClasses>")
      System.exit(0)
    }

    val sparkMaster = args(0)
    val numRows = args(1).toInt
    val numCols = args(2).toInt
    val numParts = args(3).toInt
    val numClasses = args(4).toInt

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("TSQR")
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    val sc = new SparkContext(conf)

    Thread.sleep(5000)

    val rowsPerPart = numRows / numParts
    val matrixParts = sc.parallelize(1 to numParts, numParts).mapPartitions { part =>
      val data = new Array[Double](rowsPerPart * numCols)
      var i = 0
      while (i < rowsPerPart * numCols) {
        data(i) = ThreadLocalRandom.current().nextGaussian()
        i = i + 1
      }
      val mat = new DenseMatrix[Double](rowsPerPart, numCols, data)
      Iterator(mat)
    }
    matrixParts.cache().count()

    var begin = System.nanoTime()
    val A = RowPartitionedMatrix.fromMatrix(matrixParts)
    val R = new TSQR().qrR(A)
    var end = System.nanoTime()
    logInfo("Random TSQR of " + numRows + "x" + numCols + " took " + (end - begin)/1e6 + "ms")

    var c = readChar
    sc.stop()
  }

}
