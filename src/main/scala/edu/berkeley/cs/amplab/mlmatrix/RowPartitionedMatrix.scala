package edu.berkeley.cs.amplab.mlmatrix

import java.util.concurrent.ThreadLocalRandom
import scala.reflect.ClassTag

import breeze.linalg._

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.rdd.RDD

/** Note: [[breeze.linalg.DenseMatrix]] by default uses column-major layout. */
case class RowPartition(mat: DenseMatrix[Double]) extends Serializable
case class RowPartitionInfo(
  partitionId: Int, // RDD partition this block is in
  blockId: Int, // BlockId goes from 0 to numBlocks
  startRow: Long) extends Serializable

class RowPartitionedMatrix(
  val rdd: RDD[RowPartition],
  rows: Option[Long] = None,
  cols: Option[Long] = None) extends DistributedMatrix(rows, cols) with Logging {

  // Map from partitionId to RowPartitionInfo
  // Each RDD partition can have multiple RowPartition
  @transient var partitionInfo_ : Map[Int, Array[RowPartitionInfo]] = null

  override def getDim() = {
    val dims = rdd.map { lm =>
      (lm.mat.rows.toLong, lm.mat.cols.toLong)
    }.reduce { case(a, b) =>
      (a._1 + b._1, a._2)
    }
    dims
  }

  private def calculatePartitionInfo() {
    // Partition information sorted by (partitionId, matrixInPartition)
    val rowsPerPartition = rdd.mapPartitionsWithIndex { case (part, iter) =>
      iter.zipWithIndex.map(x => (part, x._2, x._1.mat.rows.toLong))
    }.collect().sortBy(x => (x._1, x._2))

    // TODO(shivaram): Test this and make it simpler ?
    val partitionBlockStart = new collection.mutable.HashMap[Int, Int]
    partitionBlockStart.put(0, 0)
    rowsPerPartition.foreach { part =>
      if (partitionBlockStart.contains(part._1 + 1)) {
        partitionBlockStart(part._1 + 1) += 1
      } else {
        partitionBlockStart.put(part._1 + 1, partitionBlockStart(part._1) + 1)
      }
    }

    val rowsWithblockIds = rowsPerPartition.map { x =>
      (x._1, partitionBlockStart(x._1) + x._2, x._3)
    }

    val cumulativeSum = rowsWithblockIds.scanLeft(0L){ case (x1, x2) =>
      x1 + x2._3
    }.dropRight(1)

    partitionInfo_ = rowsWithblockIds.map(x => (x._1, x._2)).zip(
      cumulativeSum).map(x => RowPartitionInfo(x._1._1, x._1._2, x._2)).groupBy(x => x.partitionId)
  }

  def getPartitionInfo = {
    if (partitionInfo_ == null) {
      calculatePartitionInfo()
    }
    partitionInfo_
  }

  override def +(other: Double) = {
    new RowPartitionedMatrix(rdd.map { lm =>
      RowPartition(lm.mat :+ other)
    }, rows, cols)
  }

  override def *(other: Double) = {
    new RowPartitionedMatrix(rdd.map { lm =>
      RowPartition(lm.mat :* other)
    }, rows, cols)
  }

  override def mapElements(f: Double => Double) = {
    new RowPartitionedMatrix(rdd.map { lm =>
      RowPartition(
        new DenseMatrix[Double](lm.mat.rows, lm.mat.cols, lm.mat.data.map(f)))
    }, rows, cols)
  }

  override def aggregateElements[U: ClassTag](zeroValue: U)(seqOp: (U, Double) => U, combOp: (U, U) => U): U = {
    rdd.map { part =>
      part.mat.data.aggregate(zeroValue)(seqOp, combOp)
    }.reduce(combOp)
  }

  override def reduceRowElements(f: (Double, Double) => Double): DistributedMatrix = {
    val reducedRows = rdd.map { rowPart =>
      // get row-major layout by transposing
      val rows = rowPart.mat.data.grouped(rowPart.mat.rows).toSeq.transpose
      val reduced = rows.map(_.reduce(f)).toArray
      RowPartition(new DenseMatrix[Double](rowPart.mat.rows, 1, reduced))
    }
    new RowPartitionedMatrix(reducedRows, rows, Some(1))
  }

  override def reduceColElements(f: (Double, Double) => Double): DistributedMatrix = {
    val reducedColsPerPart = rdd.map { rowPart =>
      val cols = rowPart.mat.data.grouped(rowPart.mat.rows)
      cols.map(_.reduce(f)).toArray
    }
    val collapsed = reducedColsPerPart.reduce { case arrPair => arrPair.zipped.map(f) }
    RowPartitionedMatrix.fromArray(
      rdd.sparkContext.parallelize(Seq(collapsed), 1), Seq(1), numCols().toInt)
  }

  override def rowSums(): Seq[Double] = {
    reduceRowElements(_ + _).collect().toArray
  }

  override def colSums(): Seq[Double] = {
    reduceColElements(_ + _).collect().toArray
  }

  override def +(other: DistributedMatrix) = {
    other match {
      case otherBlocked: RowPartitionedMatrix =>
        if (this.dim == other.dim) {
          // Check if matrices share same partitioner and can be zipped
          if (rdd.partitions.size == otherBlocked.rdd.partitions.size) {
            new RowPartitionedMatrix(rdd.zip(otherBlocked.rdd).map { case (lm, otherLM) =>
              RowPartition(lm.mat :+ (otherLM.mat))
            }, rows, cols)
          } else {
            throw new SparkException(
              "Cannot add matrices with unequal partitions")
          }
        } else {
          throw new IllegalArgumentException("Cannot add matrices of unequal size")
        }
      case _ =>
        throw new IllegalArgumentException("Cannot add matrices of different types")
    }
  }

  override def apply(rowRange: Range, colRange: ::.type) = {
    // TODO: Make this a class member
    val partitionBroadcast = rdd.sparkContext.broadcast(getPartitionInfo)

    // First filter partitions which have rows in this index, then select them
    RowPartitionedMatrix.fromMatrix(rdd.mapPartitionsWithIndex { case (part, iter) =>
      val startRows = partitionBroadcast.value(part).sortBy(x => x.blockId).map(x => x.startRow)
      iter.zip(startRows.iterator).flatMap { case (lm, sr) =>
        if (sr >= rowRange.start) {
          // The end row is min of number of rows in this partition
          // and number of rows left to read
          val er = min(lm.mat.rows, (rowRange.end - sr).toInt)
          Iterator(lm.mat(0 until er, ::))
        } else {
          Iterator()
        }
      }
    })
  }

  override def apply(rowRange: ::.type, colRange: Range) = {
    new RowPartitionedMatrix(rdd.map { lm =>
      RowPartition(lm.mat(::, colRange))
    })
  }

  override def cache() = {
    rdd.cache()
    this
  }

  // TODO: This is terribly inefficient if we have more partitions.
  // Make this more efficient
  override def collect(): DenseMatrix[Double] = {
    val parts = rdd.map(x => x.mat).collect()
    parts.reduceLeft((a,b) => DenseMatrix.vertcat(a, b))
  }

  // Apply a function to each partition of the matrix
  def mapPartitions(f: DenseMatrix[Double] => DenseMatrix[Double]) = {
    // TODO: This can be efficient if we don't change num rows per partition
    RowPartitionedMatrix.fromMatrix(rdd.map { lm =>
      f(lm.mat)
    })
  }

  // def repartition ?
}

object RowPartitionedMatrix {

  // Convert an RDD[DenseMatrix[Double]] to an RDD[RowPartition]
  def fromMatrix(matrixRDD: RDD[DenseMatrix[Double]]): RowPartitionedMatrix = {
    new RowPartitionedMatrix(matrixRDD.map(mat => RowPartition(mat)))
  }

  def fromArray(matrixRDD: RDD[Array[Double]]): RowPartitionedMatrix = {
    fromMatrix(arrayToMatrix(matrixRDD))
  }

  def fromArray(
      matrixRDD: RDD[Array[Double]],
      rowsPerPartition: Seq[Int],
      cols: Int): RowPartitionedMatrix = {
    new RowPartitionedMatrix(
      arrayToMatrix(matrixRDD, rowsPerPartition, cols).map(mat => RowPartition(mat)),
        Some(rowsPerPartition.sum), Some(cols))
  }

  def arrayToMatrix(
      matrixRDD: RDD[Array[Double]],
      rowsPerPartition: Seq[Int],
      cols: Int) = {
    val rBroadcast = matrixRDD.context.broadcast(rowsPerPartition)
    val data = matrixRDD.mapPartitionsWithIndex { case (part, iter) =>
      val rows = rBroadcast.value(part)
      val matData = new Array[Double](rows * cols)
      var nRow = 0
      while (iter.hasNext) {
        val arr = iter.next()
        var idx = 0
        while (idx < arr.size) {
          matData(nRow + idx * rows) = arr(idx)
          idx = idx + 1
        }
        nRow += 1
      }
      Iterator(new DenseMatrix[Double](rows, cols, matData.toArray))
    }
    data
  }

  def arrayToMatrix(matrixRDD: RDD[Array[Double]]): RDD[DenseMatrix[Double]] = {
    val rowsColsPerPartition = matrixRDD.mapPartitionsWithIndex { case (part, iter) =>
      if (iter.hasNext) {
        val nCols = iter.next.size
        Iterator((part, 1 + iter.size, nCols))
      } else {
        Iterator((part, 0, 0))
      }
    }.collect().sortBy(x => (x._1, x._2, x._3)).map(x => (x._1, (x._2, x._3))).toMap

    val rBroadcast = matrixRDD.context.broadcast(rowsColsPerPartition)

    val data = matrixRDD.mapPartitionsWithIndex { case (part, iter) =>
      val (rows, cols) = rBroadcast.value(part)
      val matData = new Array[Double](rows * cols)
      var nRow = 0
      while (iter.hasNext) {
        val arr = iter.next()
        var idx = 0
        while (idx < arr.size) {
          matData(nRow + idx * rows) = arr(idx)
          idx = idx + 1
        }
        nRow += 1
      }
      Iterator(new DenseMatrix[Double](rows, cols, matData.toArray))
    }
    data
  }

  def createRandom(sc: SparkContext,
      numRows: Int,
      numCols: Int,
      numParts: Int,
      cache: Boolean = true): RowPartitionedMatrix = {
    val rowsPerPart = numRows / numParts
    val matrixParts = sc.parallelize(1 to numParts, numParts).mapPartitions { part =>
      val data = new Array[Double](rowsPerPart * numCols)
      var i = 0
      while (i < rowsPerPart*numCols) {
        data(i) = ThreadLocalRandom.current().nextDouble()
        i = i + 1
      }
      val mat = new DenseMatrix[Double](rowsPerPart, numCols, data)
      Iterator(mat)
    }
    if (cache) {
      matrixParts.cache()
    }
    RowPartitionedMatrix.fromMatrix(matrixParts)
  }

}
