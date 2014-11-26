package edu.berkeley.cs.amplab.mlmatrix.util

import breeze.linalg._
import breeze.generic.UFunc
import org.netlib.util.intW
import com.github.fommil.netlib.LAPACK.{getInstance=>lapack}

object QRUtils {

  /**
   * Compute a QR decomposition.
   * @returns Y, the householder reflectors
   * @returns T, the scalar factors
   * @returns R, upper triangular
   */
  def qrYTR(A: DenseMatrix[Double], cloneMatrix: Boolean = true) = {
    val m = A.rows
    val n = A.cols

    // Get optimal workspace size
    // we do this by sending -1 as lwork to the lapack function
    val scratch, work = new Array[Double](1)
    val info = new intW(0)
    lapack.dgeqrf(m, n, scratch, m, scratch, work, -1, info)
    val lwork1 = if(info.`val` != 0) n else work(0).toInt
    val workspace = new Array[Double](lwork1)

    // Perform the QR factorization with dgeqrf
    val maxd = scala.math.max(m, n)
    val mind = scala.math.min(m, n)
    val tau = new Array[Double](mind)
    val outputMat = if (cloneMatrix) {
      Utils.cloneMatrix(A)
    } else {
      A
    }
    lapack.dgeqrf(m, n, outputMat.data, m, tau, workspace, workspace.length, info)

    // Error check
    if (info.`val` > 0)
      throw new NotConvergedException(NotConvergedException.Iterations)
    else if (info.`val` < 0)
      throw new IllegalArgumentException()

    // Get R
    val R = DenseMatrix.zeros[Double](mind, n)
    var r = 0
    while (r < mind) {
      var c = r
      while (c < n) {
        R(r, c) = outputMat(r, c)
        c = c + 1
      }
      r = r + 1
    }

    (outputMat, tau, R)
  }

  /**
   * Compute R from a reduced or thin QR factorization
   */
  def qrR(A: DenseMatrix[Double], cloneMatrix: Boolean = true) = {
    qrYTR(A, cloneMatrix)._3
  }


  /**
   * Compute a reduced or thin QR factorization
   */
  def qrQR(A: DenseMatrix[Double]) : (DenseMatrix[Double], DenseMatrix[Double]) = {
    val m = A.rows
    val n = A.cols
    val mind = scala.math.min(m, n)

    val YTR = qrYTR(A)
    val Y = YTR._1
    val T = YTR._2
    val R = YTR._3

    val scratch, work = new Array[Double](1)
    val info = new intW(0)
    lapack.dorgqr(m, mind, mind, scratch, m, scratch, work, -1, info)
    val lwork2 = if(info.`val` != 0) n else work(0).toInt
    val workspace2 = new Array[Double](lwork2)

    // Get Q from the matrix returned by dgep3
    val Q = DenseMatrix.zeros[Double](m, mind)
    lapack.dorgqr(m, mind, mind, Y.data, m, T, workspace2, workspace2.length, info)

    var r = 0
    while (r < m) {
      var c = 0
      while (c < mind) {
        Q(r, c) = Y(r, c)
        c = c + 1
      }
      r = r + 1
    }

    // Error check
    if (info.`val` > 0)
      throw new NotConvergedException(NotConvergedException.Iterations)
    else if (info.`val` < 0)
      throw new IllegalArgumentException()

    (Q, R)
  }


  /**
   * Compute Q %*% b or t(Q) %*% b using householder reflectors Y and scalar factors T
   */
  def applyQ(
      Y: DenseMatrix[Double],
      T: Array[Double],
      bPart: DenseMatrix[Double],
      transpose: Boolean) = {

    val result = if (bPart.rows == Y.rows && bPart.cols == Y.cols) {
        Utils.cloneMatrix(bPart)
      } else {
        val r = new DenseMatrix[Double](Y.rows, bPart.cols)
        for (i <- 0 until bPart.rows) {
          for (j <- 0 until bPart.cols) {
            r(i, j) = bPart(i, j)
          }
        }
        r
      }

    val work = new Array[Double](1)
    val info = new intW(0)
    val trans = if (transpose) "T" else "N"

    lapack.dormqr("L", trans, result.rows, result.cols, T.length, Y.data, Y.rows, T,
      result.data, result.rows, work, -1, info)

    val lwork1 = if(info.`val` != 0) result.cols else work(0).toInt
    val workspace = new Array[Double](lwork1)

    lapack.dormqr("L", trans, result.rows, result.cols, T.length, Y.data, Y.rows, T,
      result.data, result.rows, workspace, workspace.length, info)

    if (info.`val` > 0)
      throw new NotConvergedException(NotConvergedException.Iterations)
    else if (info.`val` < 0)
      throw new IllegalArgumentException()

    // Select only the first N rows if we multiplied by t(Q)
    if (transpose) {
      val rowsToSelect = min(Y.cols, result.rows)
      result(0 until rowsToSelect, ::)
    } else {
      result
    }
  }

  def qrSolve(A: DenseMatrix[Double], b: DenseMatrix[Double]) = {
    val (yPart, tau, rPart) = qrYTR(A)
    val x = applyQ(yPart, tau, b, transpose=true)

    // Resize x to only take first N rows
    (rPart, x)
  }

  def qrSolveMany(A: DenseMatrix[Double], bs: Seq[DenseMatrix[Double]]) = {
    val (yPart, tau, rPart) = qrYTR(A)
    val xs = bs.map { b =>
      applyQ(yPart, tau, b, transpose=true)
    }
    (rPart, xs)
  }

}
