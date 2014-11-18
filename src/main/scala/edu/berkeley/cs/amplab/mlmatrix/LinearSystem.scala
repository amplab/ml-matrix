package edu.berkeley.cs.amplab.mlmatrix

import breeze.linalg._
import breeze.numerics._

import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

case class LinearSystem(
  A: RowPartitionedMatrix,
  b: RowPartitionedMatrix,
  xCorrect: DenseMatrix[Double],
  condNumber: Double,
  theta: Double) extends Logging {

  def computeResidualNorm(xComputed: DenseMatrix[Double]) = {
    val xBroadcast = A.rdd.context.broadcast(xComputed)
    val axComputed = A.mapPartitions { part =>
      part*xBroadcast.value
    }
    val residualNorm = (b - axComputed).normFrobenius()
    logInfo("Theta: " + theta + " condNumber " + condNumber + " 2-norm of residual is " +
      residualNorm)
    residualNorm
  }

  def computeRelativeError(xComputed: DenseMatrix[Double]) = {
    val relativeError = norm((xComputed - xCorrect).toDenseVector)/norm(xCorrect.toDenseVector)
    logInfo("Theta: " + theta + " condNumber: " + condNumber + " relative error is " +
      relativeError)
    relativeError
  }
}

object LinearSystem {

  /**
    * Create several linear systems with varying condition numbers and thetas, where
    * theta is the angle between the vector b and the range of A. We create a
    * matrix A with a specific condition number by first setting the entries of a
    * diagonal matrix to be the desired singular values of A, and then hitting this
    * diagonal matrix with orthogonal matrices U and V on the left and right respectively.
    * When we create the matrix U, we create a matrix with one additional column, called qe.
    * The vector qe is then orthogonal to the range of A and we can use qe to create a
    * vector b at the specified angle theta away from the range of A.
    */
  def createLinearSystems(
    sc: SparkContext,
    numRows: Int,
    numCols: Int,
    numClasses: Int,
    numParts: Int,
    condNumbers: Seq[Double],
    thetas: Seq[Double]): Seq[LinearSystem] = {

    // Create a random matrix with specific condition number
    val Right = DenseMatrix.rand(numCols, numCols)
    // Only use the orthogonal part of this matrix, throwing away R
    val V = QRUtils.qrQR(Right)._1

    val Left = RowPartitionedMatrix.createRandom(sc, numRows, numCols + 1, numParts)
    // FIXME: we are throwing away rLeft, there could be a more efficient way to do this
    val (u, rLeft) = new TSQR().qrQR(Left)

    val systems = condNumbers.flatMap { condNumber =>
      thetas.map { theta =>
        var diag = DenseMatrix.zeros[Double](numCols, numCols)
        (0 until numCols).foreach { c =>
          diag(c, c) = math.pow(condNumber, -1.0 * c.toDouble/numCols)
        }

        // Multiply diagonal matrix with entries diag by V
        val SigmaV = diag*V

        // A = U*Sigma*V
        val sigmaVBroadcast = sc.broadcast(SigmaV)
        val numColsBroadcast = sc.broadcast(numCols)

        val A = u.mapPartitions { part =>
          part(::, 0 until (part.cols-1))*sigmaVBroadcast.value
        }

        // Get the last column of u, which will be orthogonal to column space of A
        val qe = u(::, (u.numCols().toInt-1) until u.numCols().toInt)

        // FIXME: Avoid broadcast by seeding the random number generator
        // to make rand deterministic and creating each x locally
        val x = DenseMatrix.rand(numCols, numClasses)
        val xBroadcast = sc.broadcast(x)

        val bCorrect = A.mapPartitions(part => part*xBroadcast.value)
        val lengthBCorrect = bCorrect.normFrobenius()

        // Add component that is orthogonal to the column space of A
        val b = bCorrect*Math.cos(theta) + qe*Math.sin(theta)*lengthBCorrect

        LinearSystem(A, b.asInstanceOf[RowPartitionedMatrix], x, condNumber, theta)
      }
    }

    systems
  }

}
