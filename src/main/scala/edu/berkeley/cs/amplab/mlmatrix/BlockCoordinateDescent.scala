package edu.berkeley.cs.amplab.mlmatrix

import breeze.linalg._

import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class BlockCoordinateDescent(colBlockSize: Int, numEpochs: Int) extends Logging with Serializable {

  def solveOnePassL2(
      aParts: Iterator[RowPartitionedMatrix],
      b: RowPartitionedMatrix,
      lambdas: Array[Double],
      solver: RowPartitionedSolver): Iterator[Seq[DenseMatrix[Double]]] = {
    // Each partition of output is a Seq[DenseMatrix[Double]]
    // with one entry per lambda
    var output = b.rdd.map { part =>
      val nrows = part.mat.rows
      val ncols = part.mat.cols
      val arr = new Array[DenseMatrix[Double]](lambdas.length)
      (0 until lambdas.length).foreach { l =>
        arr(l) = new DenseMatrix[Double](nrows, ncols)
      }
      arr.toSeq
    }.cache()

    // Step 2:
    try {
      val models = aParts.map { aPart =>
        aPart.cache()

        // Compute Aj \ (b - output + AjXj)
        //
        // NOTE: In the one pass case, Xj is always zero.
        // So we just compute (b - output)
        val bOutput = b.rdd.zip(output).map { part =>
          part._2.map { out =>
            part._1.mat - out
          }
        }

        val newXjs = solver.solveManyLeastSquaresWithL2(aPart, bOutput, lambdas)

        // Update output
        val newXBroadcast = b.rdd.context.broadcast(newXjs)
        val newOutput = aPart.rdd.zip(output).map { part =>
          val xs = newXBroadcast.value
          // Subtract the oldAx and add the newAx
          // NOTE: oldAx is zero in the one pass case
          var i = 0
          while (i < xs.length) {
            val newAx = part._1.mat * xs(i)
            part._2(i) += newAx
            i = i + 1
          }
          part._2
        }.cache()

        // Materialize this output and remove the older output
        newOutput.count()
        output.unpersist()
        aPart.rdd.unpersist()

        newXBroadcast.unpersist()
        output = newOutput

        newXjs
      }
      models
    } finally {
      output.unpersist()
    }
  }

  def solveLeastSquaresWithL2(
    aParts: Seq[RowPartitionedMatrix],
    b: RowPartitionedMatrix,
    lambdas: Array[Double],
    numIters: Int,
    solver: RowPartitionedSolver): Seq[Seq[DenseMatrix[Double]]]  = {

    val numColBlocks = aParts.length
    val numColsb = b.numCols()

    // TODO: This is inefficient as we can pre-compute AtAs if we using NormalEquations and
    // reuse it across iterations. However the RowPartitionedSolver interface doesn't allow us
    // to do that yet.
    // Each partition of output is a DenseMatrix[Double]
    var output = b.rdd.map { part =>
      val nrows = part.mat.rows
      val ncols = part.mat.cols
      val arr = new Array[DenseMatrix[Double]](lambdas.length)
      (0 until lambdas.length).foreach { l =>
        arr(l) = new DenseMatrix[Double](nrows, ncols)
      }
      arr.toSeq
    }.cache()

    val xs = (0 until numColBlocks).map { colBlock =>
      (0 until lambdas.length).map { l =>
        new DenseMatrix[Double](aParts(colBlock).numCols().toInt, numColsb.toInt)
      }.to[scala.collection.Seq]
    }.toArray

    (0 until numIters).foreach { iter =>
      // Step 2: Pick a random permutation
      val permutation = scala.util.Random.shuffle((0 until numColBlocks).toList)
      permutation.foreach { p =>
        val aPart = aParts(p)

        // Solve A \ (b - output + AjXj)
        val AbOutput = aPart.rdd.zip(b.rdd.zip(output))

        val xsBroadcast = b.rdd.context.broadcast(xs(p))
        val updatedB = AbOutput.map { part =>
          val xsB = xsBroadcast.value
          xsB.zip(part._2._2).map { case (xsValue, outPart) =>
            // First compute AjXj. Then add 'b' and subtract 'output'
            val ax = part._1.mat * (xsValue)
            ax :+= (part._2._1.mat)
            ax :-= (outPart)
            ax
          }.to[scala.collection.Seq]
        }

        // Local solve
        val newXjs = solver.solveManyLeastSquaresWithL2(aPart, updatedB, lambdas)

        // Update output
        val newXBroadcast = b.rdd.context.broadcast(newXjs)
        val newOutput = aPart.rdd.zip(output).map { part =>
          val xsB = xsBroadcast.value
          val newXsB = newXBroadcast.value

          part._2.zip(xsB.zip(newXsB)).map { case (outPart, x) =>
            // Subtract the oldAx and add the newAx
            val diff = part._1.mat * (x._1)
            val newAx = part._1.mat * (x._2)
            diff :-= newAx
            outPart - diff
          }
        }.cache()

        // Materialize this output and remove the older output
        newOutput.count()
        output.unpersist()

        xsBroadcast.unpersist()
        newXBroadcast.unpersist()
        output = newOutput

        // Set the newX
        xs(p) = newXjs
      }
    }
    xs
  }
}
