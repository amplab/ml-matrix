package edu.berkeley.cs.amplab.mlmatrix

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer

import breeze.linalg._

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PartitionPruningRDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkException

case class BlockPartition(
  blockIdRow: Int,
  blockIdCol: Int,
  mat: DenseMatrix[Double]) extends Serializable

// Information about BlockPartitionedMatrix maintained on the driver
case class BlockPartitionInfo(
  partitionId: Int,
  blockIdRow: Int,
  blockIdCol: Int,
  startRow: Long,
  numRows: Int,
  startCol: Long,
  numCols: Int) extends Serializable

class BlockPartitionedMatrix(
    val numRowBlocks: Int,
    val numColBlocks: Int,
    val rdd: RDD[BlockPartition]) extends DistributedMatrix with Logging {

  @transient var blockInfo_ : Map[(Int, Int), BlockPartitionInfo] = null

  override def getDim = {
    val bi = getBlockInfo
  
    val xDim = bi.map { x =>
      (x._1._1, x._2.numRows.toLong)
    }.groupBy(x => x._1).values.map { x =>
      x.head._2.toLong
    }.reduceLeft {
      _ + _
    }

    val yDim = bi.map { x =>
      (x._1._2, x._2.numCols.toLong)
    }.groupBy(x => x._1).values.map { x =>
      x.head._2.toLong
    }.reduceLeft {
      _ + _
    }

    (xDim, yDim)
  }

  private def calculateBlockInfo() {
    // TODO: Part of this is repeated in the fromArray code. See if we can avoid this
    // duplication.

    val blockStartRowColsParts = rdd.mapPartitionsWithIndex { case (partId, iter) =>
      val partDimIter = iter.map { part =>
        val dims = new ArrayBuffer[(Int, Int, Long, Long)]
        val partIdMap = (part.blockIdRow, part.blockIdCol) -> partId

        dims += ((part.blockIdRow, part.blockIdCol,
                 part.mat.rows.toLong, part.mat.cols.toLong))

        (partIdMap, dims)
      }
      partDimIter
    }.collect()
    val blockStartRowCols = blockStartRowColsParts.flatMap(x => x._2).sortBy(x => (x._1, x._2))

    val rowReps = blockStartRowCols.groupBy(x => x._1).values.map { x =>
      x.head
    }.toSeq.sortBy{ x =>
      (x._1, x._2)
    }

    val colReps = blockStartRowCols.groupBy(x => x._2).values.map { x =>
      x.head
    }.toSeq.sortBy{ x =>
      (x._1, x._2)
    }

    // Calculate startRows
    val cumulativeRowSum = rowReps.scanLeft(0L) { case(x1, x2) =>
      x1 + x2._3
    }.dropRight(1)

    val rowStarts = rowReps.zip(cumulativeRowSum).map { x =>
      (x._1._1, (x._1._3, x._2))
    }.toMap

    val cumulativeColSum = colReps.scanLeft(0L) { case(x1, x2) =>
      x1 + x2._4
    }.dropRight(1)

    val colStarts = colReps.zip(cumulativeColSum).map { x =>
      (x._1._2, (x._1._4, x._2))
    }.toMap

    val partitionIdsMap = blockStartRowColsParts.map(x => x._1).toMap

    blockInfo_ = rowStarts.keys.flatMap { r =>
      colStarts.keys.filter { c =>
        partitionIdsMap.contains(r, c)
      }.map { c =>
        ((r,c), BlockPartitionInfo(partitionIdsMap((r,c)),
          r, c, rowStarts(r)._2, rowStarts(r)._1.toInt, colStarts(c)._2, colStarts(c)._1.toInt))
      }
    }.toMap
  }

  def getBlockInfo = {
    if (blockInfo_ == null) {
      calculateBlockInfo()
    }
    blockInfo_
  }

  override def +(other: Double) = {
    new BlockPartitionedMatrix(numRowBlocks, numColBlocks, rdd.map { lm =>
      BlockPartition(lm.blockIdRow, lm.blockIdCol, lm.mat :+ other)
    })
  }

  override def *(other: Double) = {
    new BlockPartitionedMatrix(numRowBlocks, numColBlocks, rdd.map { lm =>
      BlockPartition(lm.blockIdRow, lm.blockIdCol, lm.mat :* other)
    })
  }

  override def mapElements(f: Double => Double) = {
    new BlockPartitionedMatrix(numRowBlocks, numColBlocks, rdd.map { lm =>
      BlockPartition(lm.blockIdRow, lm.blockIdCol,
        new DenseMatrix[Double](lm.mat.rows, lm.mat.cols, lm.mat.data.map(f)))
    })
  }

  override def aggregateElements[U: ClassTag](zeroValue: U)(seqOp: (U, Double) => U, combOp: (U, U) => U): U = {
    rdd.map { part =>
      part.mat.data.aggregate(zeroValue)(seqOp, combOp)
    }.reduce(combOp)
  }

  override def reduceRowElements(f: (Double, Double) => Double): DistributedMatrix = {
    val blockReduced = rdd.map { block =>
      val rows = block.mat.data.grouped(block.mat.cols)
      val reduced = rows.map(_.reduce(f)).toArray
      BlockPartition(block.blockIdRow, block.blockIdCol,
        new DenseMatrix[Double](block.mat.rows, 1, reduced)
      )
    }

    def rowWiseReduce(block1: Array[Double], block2: Array[Double]): Array[Double] = {
      block1.zip(block2).map { case (d1, d2) => f(d1, d2) }
    }

    val reduced = blockReduced
      .map { block => (block.blockIdRow, block.mat.data) }
      .groupByKey(numRowBlocks)
      .map { case (blockRow, blocks) =>
        val reducedBlocks = blocks.reduce(rowWiseReduce).toArray
        BlockPartition(blockRow, 0, new DenseMatrix[Double](reducedBlocks.length, 1, reducedBlocks))
      }

    new BlockPartitionedMatrix(numRowBlocks, 1, reduced)
  }

  override def reduceColElements(f: (Double, Double) => Double): DistributedMatrix = {
    val blockReduced = rdd.map { block =>
      val cols = block.mat.data.grouped(block.mat.cols).toArray.transpose
      val reduced = cols.map(_.reduce(f))
      BlockPartition(block.blockIdRow, block.blockIdCol,
        new DenseMatrix[Double](1, block.mat.cols, reduced)
      )
    }

    def colWiseReduce(block1: Array[Double], block2: Array[Double]): Array[Double] = {
      block1.zip(block2).map { case (d1, d2) => f(d1, d2) }
    }

    val reduced = blockReduced
      .map { block => (block.blockIdCol, block.mat.data) }
      .groupByKey(numColBlocks)
      .map { case (blockCol, blocks) =>
        val reducedBlocks = blocks.reduce(colWiseReduce).toArray
        BlockPartition(0, blockCol, new DenseMatrix[Double](1, reducedBlocks.length, reducedBlocks))
    }

    new BlockPartitionedMatrix(1, numColBlocks, reduced)
  }

  override def +(other: DistributedMatrix) = {
    other match {
      // We really need a function to check if two matrices are partitioned similarly
      case otherBlocked: BlockPartitionedMatrix =>
        if (getBlockInfo == otherBlocked.getBlockInfo) {
          // TODO: Optimize if the blockIds are in the same order.
          val blockRDD = rdd.map(x => ((x.blockIdRow, x.blockIdCol), x.mat)).join {
            otherBlocked.rdd.map(y => ((y.blockIdRow, y.blockIdCol), y.mat))
          }.map { x =>
            new BlockPartition(x._1._1, x._1._2, x._2._1 + x._2._2)
          }
          new BlockPartitionedMatrix(numRowBlocks, numColBlocks, blockRDD)
        } else {
          throw new SparkException(
            "Cannot add matrices with unequal partitions")
        }
      case _ =>
        throw new IllegalArgumentException("Cannot add matrices of different types")
    }
  }

  def normFro() = {
    math.sqrt(rdd.map{ lm => lm.mat.data.map(x => math.pow(x, 2)).sum }.reduce(_ + _))
  }

  override def apply(rowRange: Range, colRange: ::.type) = {
    val blockInfos = getBlockInfo
    val blockInfosBcast = rdd.context.broadcast(blockInfos)

    val blocksWithRows = blockInfos.filter { bi =>
      rowRange.filter(i => i >= bi._2.startRow && i < bi._2.startRow + bi._2.numRows).nonEmpty
    }.values.toSeq.sortBy(x => x.blockIdRow)

    val blocksFilteredIds = blocksWithRows.map(bi => bi.partitionId).toSet
    val prunedRdd = PartitionPruningRDD.create(rdd, part => blocksFilteredIds.contains(part))

    // Renumber the blockIdRows from 0 to number of row blocks
    val newBlockIdMap = blocksWithRows.map(x => x.blockIdRow).distinct.zipWithIndex.toMap

    val newBlockIdBcast = rdd.context.broadcast(newBlockIdMap)

    val blockRDD = prunedRdd.filter { part =>
      newBlockIdBcast.value.contains(part.blockIdRow)
    }.map { part =>
      // Get a new blockIdRow, keep same blockIdCol and update the matrix
      val newBlockIdRow = newBlockIdBcast.value(part.blockIdRow)
      val blockInfo = blockInfosBcast.value((part.blockIdRow, part.blockIdCol))

      val validIdx = rowRange.filter { i =>
        i >= blockInfo.startRow && i < blockInfo.startRow + blockInfo.numRows
      }

      val localIdx = validIdx.map(x => x - blockInfo.startRow).map(x => x.toInt)
      val newMat = part.mat(localIdx.head to localIdx.last, ::)
      BlockPartition(newBlockIdRow, blockInfo.blockIdCol, newMat)
    }
    new BlockPartitionedMatrix(newBlockIdMap.size, numColBlocks, blockRDD)
  }

  override def apply(rowRange: ::.type, colRange: Range) = {
    val blockInfos = getBlockInfo
    val blockInfosBcast = rdd.context.broadcast(blockInfos)

    val blocksWithCols = blockInfos.filter { bi =>
      colRange.filter(i => i >= bi._2.startCol && i < bi._2.startCol + bi._2.numCols).nonEmpty
    }.values.toSeq.sortBy(x => x.blockIdCol)

    val blocksFilteredIds = blocksWithCols.map(bi => bi.partitionId).toSet
    val prunedRdd = PartitionPruningRDD.create(rdd, part => blocksFilteredIds.contains(part))

    // Renumber the blockIdRows from 0 to number of row blocks
    val newBlockIdMap = blocksWithCols.map(x => x.blockIdCol).distinct.zipWithIndex.toMap
    val newBlockIdBcast = rdd.context.broadcast(newBlockIdMap)

    val blockRDD = prunedRdd.filter { part =>
      newBlockIdBcast.value.contains(part.blockIdCol)
    }.map { part =>
      // Get a new blockIdRow, keep same blockIdCol and update the matrix
      val newBlockIdCol = newBlockIdBcast.value(part.blockIdCol)
      val blockInfo = blockInfosBcast.value((part.blockIdRow, part.blockIdCol))

      val validIdx = colRange.filter { i =>
        i >= blockInfo.startCol && i < blockInfo.startCol + blockInfo.numCols
      }

      val localIdx = validIdx.map(x => x - blockInfo.startCol).map(x => x.toInt)
      val newMat = part.mat(::, localIdx.head to localIdx.last)
      BlockPartition(blockInfo.blockIdRow, newBlockIdCol, newMat)
    }
    new BlockPartitionedMatrix(numRowBlocks, newBlockIdMap.size, blockRDD)
  }

  override def apply(rowRange: Range, colRange: Range) = ???

  override def cache() = {
    rdd.cache()
    this
  }

  // TODO: This is terribly inefficient if we have more partitions.
  // Make this more efficient
  override def collect(): DenseMatrix[Double] = {
    val parts = rdd.map(x => ((x.blockIdRow, x.blockIdCol), x.mat)).collect()
    val dims = getDim
    val mat = new DenseMatrix[Double](dims._1.toInt, dims._2.toInt)
    val blockInfos = getBlockInfo
    parts.foreach { part =>
      val blockInfo = blockInfos((part._1._1, part._1._2))
      // Figure out where this part should be put
      val rowRange = 
        blockInfo.startRow.toInt until (blockInfo.startRow + blockInfo.numRows).toInt
      val colRange = 
        blockInfo.startCol.toInt until (blockInfo.startCol + blockInfo.numCols).toInt
      mat(rowRange, colRange) := part._2
    }
    mat
  }

  def getBlockRange(
      startRowBlock: Int,
      endRowBlock: Int,
      startColBlock: Int,
      endColBlock: Int) = {
    val blockInfos = getBlockInfo
    val blocksFiltered = blockInfos.filter { bi =>
      bi._2.blockIdRow >= startRowBlock && bi._2.blockIdRow < endRowBlock &&
      bi._2.blockIdCol >= startColBlock && bi._2.blockIdCol < endColBlock
    }
    val blocksFilteredIds = blocksFiltered.values.map(bi => bi.partitionId).toSet

    val newBlockIds = blocksFiltered.mapValues { bi =>
      (bi.blockIdRow - startRowBlock, bi.blockIdCol - startColBlock)
    }.map(identity)

    val newBlockIdBcast = rdd.context.broadcast(newBlockIds)

    val prunedRdd = PartitionPruningRDD.create(rdd, part => blocksFilteredIds.contains(part))

    val blockRDD = prunedRdd.filter { part =>
      newBlockIdBcast.value.contains((part.blockIdRow, part.blockIdCol))
    }.map { part =>
      // Get a new blockIdRow, blockIdCol
      val newBlockIds = newBlockIdBcast.value((part.blockIdRow, part.blockIdCol))
      BlockPartition(newBlockIds._1, newBlockIds._2, part.mat)
    }
    new BlockPartitionedMatrix(endRowBlock - startRowBlock, endColBlock - startColBlock, blockRDD)
  }

  // Get a single column block as a row partitioned matrix
  def getColBlock(colBlock: Int) : RowPartitionedMatrix =  {
    val blockRDD = getBlockRange(0, numRowBlocks, colBlock, colBlock + 1)
    RowPartitionedMatrix.fromMatrix(blockRDD.rdd.map(_.mat))
  }
}

object BlockPartitionedMatrix {

  // def fromColumnBlocks(colBlocks: Seq[RowPartitionedMatrix])
  // def fromRowBlocks(rowBlocks: Seq[ColumnPartitionedMatrix])

  // Assumes each row is represented as an array of Doubles
  def fromArray(
    matrixRDD: RDD[Array[Double]],
    numRowsPerBlock: Int,
    numColsPerBlock: Int): BlockPartitionedMatrix = {

    // Collect how many rows are there in each partition of RDD
    val perPartDims = matrixRDD.mapPartitionsWithIndex { case (part, iter) =>
      var numRows = 0L
      var numCols = 0
      while (iter.hasNext) {
        numRows += 1L
        numCols = iter.next().length
      }
      Iterator.single( (part, numRows, numCols) )
    }.collect().sortBy(x => x._1)

    val cumulativeSum = perPartDims.scanLeft(0L){ case(x1, x2) =>
      x1 + x2._2
    }
    val numRows = cumulativeSum.takeRight(1).head

    val rowStarts = perPartDims.zip(cumulativeSum.dropRight(1)).map { x =>
      (x._1._1, (x._2, x._1._3))
    }.toMap
    val rowStartsBroadcast = matrixRDD.context.broadcast(rowStarts)
    val numColBlocks = math.ceil(
      rowStarts.head._2._2.toFloat / numColsPerBlock.toFloat).toInt
    val numRowBlocks = math.ceil(numRows / numRowsPerBlock).toInt

    val blockRDD = matrixRDD.mapPartitionsWithIndex { case (part, iter) =>
      val startRow = rowStartsBroadcast.value(part)._1
      val ret = new ArrayBuffer[(Int, Array[Double])]
      var rowNo = 0L
      while (iter.hasNext) {
        // For each row
        val rowBlock = ((rowNo + startRow) / numRowsPerBlock).toInt
        val arr = iter.next()
        rowNo += 1
        (0 until numColBlocks).foreach { col =>
          ret.append(
            (rowBlock * numColBlocks + col,
             arr.slice(col * numColsPerBlock, (col + 1) * numColsPerBlock))
          )
        }
      }
      ret.iterator
    }.groupByKey(numColBlocks * numRowBlocks).map { item =>
      val matData = new ArrayBuffer[Double]
      var numRows = 0
      var numCols = 0
      val iter = item._2.iterator
      while (iter.hasNext) {
        val arr = iter.next()
        matData ++= arr
        numRows += 1
        numCols = arr.length
      }
      new BlockPartition(item._1 / numColBlocks, item._1 % numColBlocks,
        new DenseMatrix[Double](numCols, numRows, matData.toArray).t)
    }

    new BlockPartitionedMatrix(numRowBlocks, numColBlocks, blockRDD)
  }
}
