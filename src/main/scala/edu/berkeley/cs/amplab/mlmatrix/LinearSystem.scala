package edu.berkeley.cs.amplab.mlmatrix


import breeze.linalg._
import breeze.numerics._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import edu.berkeley.cs.amplab.mlmatrix.util.Utils


case class LinearSystem(
  A: RowPartitionedMatrix,
  b: RowPartitionedMatrix,
  xCorrect: DenseMatrix[Double],
  condNumber: Double,
  theta: Double) {

    // val ALocal = A.collect()
    // val bLocal = b.collect()
    def computeResidualNorm(xComputed: DenseMatrix[Double]) = {
      val xBroadcast = A.rdd.context.broadcast(xComputed)
      val axComputed = A.asInstanceOf[RowPartitionedMatrix].mapPartitions { part =>
        part*xBroadcast.value
      }
      val residualNorm = norm((b.collect() - axComputed.collect()).toDenseVector)
      println("Theta: " + theta + " condNumber " + condNumber + " 2-norm of residual is " + residualNorm)
      residualNorm
    }
    def computeRelativeError(xComputed: DenseMatrix[Double]) = {
      val relativeError = norm((xComputed - xCorrect).toDenseVector)/norm(xCorrect.toDenseVector)
      println("Theta: " + theta + " condNumber: " + condNumber + " relative error is " + relativeError)
      relativeError
    }
  }

object LinearSystem {

  def createLinearSystem(
    sc: SparkContext,
    numRows: Int,
    numCols: Int,
    numClasses: Int,
    numParts: Int,
    condNumber: Double,
    theta: Double): LinearSystem = {

    //Create a random matrix with specific condition number
    val Right = DenseMatrix.rand(numCols, numCols)
    //Only use the orthogonal part of this matrix, throwing away R
    val V = QRUtils.qrQ(Right)
    var diag = DenseMatrix.zeros[Double](numCols, numCols)
    (0 until numCols).foreach { c =>
      diag(c, c) = math.pow(condNumber, -1.0 * c.toDouble/numCols)
    }
    // Multiply diagonal matrix with entries diag by V
    val SigmaV = diag * V

    val Left = RowPartitionedMatrix.createRandom(sc, numRows, numCols + 1, numParts)
    // FIXME, we are throwing away rLeft, there could be a more efficient way to do this
    //Assume Tall & Skinny Matrix here
    val (u, rLeft) = new TSQR().qrQR(Left)

    // A = U*Sigma*V
    val sigmaVBroadcast = sc.broadcast(SigmaV)
    val numColsBroadcast = sc.broadcast(numCols)
    val A = u.mapPartitions { part =>
      part(::, 0 until (part.cols-1))*sigmaVBroadcast.value
    }

    //Get the last column of u, which will be orthogonal to column space of A
    val qe = u(::, (u.numCols().toInt-1) until u.numCols().toInt)

    //FixME: Avoid broadcast by seeding the random number generator to make rand deterministic
    //and creating each x locally
    val x = DenseMatrix.rand(numCols, numClasses)
    val xBroadcast = sc.broadcast(x)

    println("xCorrect has " + x.rows + " rows and " + x.cols + " columns.")
    val bCorrect = A.mapPartitions(part => part*xBroadcast.value)
    val lengthBCorrect = norm(bCorrect.collect().toDenseVector)

    //Add component that is orthogonal to the column space of A
    val b = bCorrect*Math.cos(theta) + qe*(lengthBCorrect*Math.sin(theta))
    //println("Eta is " + norm(A)*norm(x)/norm(bCorrect))

    LinearSystem(A, b.asInstanceOf[RowPartitionedMatrix], x, condNumber, theta)
  }

  def createLinearSystems(
    sc: SparkContext,
    numRows: Int,
    numCols: Int,
    numClasses: Int,
    numParts: Int,
    condNumbers: Seq[Double],
    thetas: Seq[Double]): Seq[LinearSystem] = {

    //Create a random matrix with specific condition number
    val Right = DenseMatrix.rand(numCols, numCols)
    //Only use the orthogonal part of this matrix, throwing away R
    val V = QRUtils.qrQ(Right)

    val Left = RowPartitionedMatrix.createRandom(sc, numRows, numCols + 1, numParts)
    //FIXME, we are throwing away rLeft, there could be a more efficient way to do this
    val (u, rLeft) = new TSQR().qrQR(Left)

    val systems = condNumbers.flatMap { condNumber =>
      thetas.map { theta =>
        var diag = DenseMatrix.zeros[Double](numCols, numCols)
        (0 until numCols).foreach { c =>
          diag(c, c) = math.pow(condNumber, -1.0 * c.toDouble/numCols)
        }
        //println("Inputted condition number is " + condNumber)
        //println("Ratio of singular values is " + diag(0,0) / diag(numCols-1, numCols-1))

        //Multiply diagonal matrix with entries diag by V
        //val SigmaV = V.mulColumnVector(diag)
        val SigmaV = diag*V

        //A = U*Sigma*V
        val sigmaVBroadcast = sc.broadcast(SigmaV)
        val numColsBroadcast = sc.broadcast(numCols)

        val A = u.mapPartitions { part =>
          part(::, 0 until (part.cols-1))*sigmaVBroadcast.value
        }

        //Get the last column of u, which will be orthogonal to column space of A
        val qe = u(::, (u.numCols().toInt-1) until u.numCols().toInt)

        // FixME: Avoid broadcast by seeding the random number generator
        // to make rand deterministic and creating each x locally
        val x = DenseMatrix.rand(numCols, numClasses)
        val xBroadcast = sc.broadcast(x)

        val bCorrect = A.mapPartitions(part => part*xBroadcast.value)
        val lengthBCorrect = norm(bCorrect.collect().toDenseVector)

        //Add component that is orthogonal to the column space of A
        val b = bCorrect*Math.cos(theta) + qe*Math.sin(theta)*lengthBCorrect
        //println("Eta is " + norm(A)*norm(x)/norm(bCorrect))

        LinearSystem(A, b.asInstanceOf[RowPartitionedMatrix], x, condNumber, theta)
      }
    }

    systems
  }

}
