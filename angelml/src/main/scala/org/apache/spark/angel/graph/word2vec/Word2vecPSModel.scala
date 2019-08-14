package org.apache.spark.angel.graph.word2vec


import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc
import com.tencent.angel.sona.context.PSContext
import org.apache.spark.angel.psf.embedding.bad._
import org.apache.spark.angel.graph.utils.NEModel
import org.apache.spark.angel.graph.utils.NEModel.NEDataSet
import org.apache.spark.angel.graph.word2vec.Word2vecPSModel.W2VDataSet
import org.apache.spark.angel.psf.embedding.NEDot.NEDotResult
import org.apache.spark.angel.psf.embedding.w2v.{Adjust, AdjustParam, Dot, DotParam}
import org.apache.spark.rdd.RDD

import scala.util.Random


class Word2vecPSModel(embeddingMatName: String,
                      numNode: Int,
                      dimension: Int,
                      model: String,
                      numPart: Int,
                      numEpoch: Int,
                      windowSize: Int,
                      negSample: Int,
                      maxLength: Int,
                      learningRate: Double,
                      batchSize: Int,
                      numNodesPerRow: Int,
                      seed: Int)
  extends NEModel(numNode, dimension, numPart, numNodesPerRow, 2, false, seed) {

  lazy val modelId: Int = model match {
    case "skipgram" => 0
    case "cbow" => 1
    case _ => throw new AngelException("model type should be cbow or skipgram")
  }

  private def sgdForBatch(partitionId: Int,
                          seed: Int,
                          batchId: Int,
                          negative: Int,
                          batch: NEDataSet): (Double, Long, Array[Long]) = {
    var (start, end) = (0L, 0L)

    // dot
    start = System.currentTimeMillis()
    val dots = psfGet(getDotFunc(batch, seed, negative, partitionId))
      .asInstanceOf[NEDotResult].result
    end = System.currentTimeMillis()
    val dotTime = end - start

    // gradient
    start = System.currentTimeMillis()
    val loss = doGrad(dots, negative, learningRate.toFloat)
    end = System.currentTimeMillis()
    val gradientTime = end - start

    // adjust
    start = System.currentTimeMillis()
    psfUpdate(getAdjustFunc(batch, seed, negative, dots, partitionId))
    end = System.currentTimeMillis()
    val adjustTime = end - start

    // return loss
    if ((batchId + 1) % 100 == 0)
      NEModel.logTime(s"batchId=$batchId dotTime=$dotTime gradientTime=$gradientTime adjustTime=$adjustTime")

    (loss, dots.length.toLong, Array(dotTime, gradientTime, adjustTime))
  }

  private def sgdForPartition(partitionId: Int,
                              iterator: Iterator[NEDataSet],
                              negative: Int,
                              alpha: Float): Iterator[(Double, Long, Array[Long])] = {
    PSContext.instance()
    iterator.zipWithIndex.map { case (batch, index) =>
      sgdForBatch(partitionId, rand.nextInt(), index, negSample, batch)
    }
  }

  def train(corpus: RDD[Array[Int]]): Unit = {
    psfUpdate(getInitFunc(corpus.getNumPartitions, numNode, maxLength, negSample, windowSize))
    val trainBatches = Word2vecPSModel.buildDataBatches(corpus, batchSize)

    for (epoch <- 1 to numEpoch) {
      val data = trainBatches.next()
      // val numPartitions = data.getNumPartitions
      val middle = data.mapPartitionsWithIndex((partitionId, iterator) =>
        sgdForPartition(partitionId, iterator, negSample, learningRate.toFloat),
        preservesPartitioning = true
      ).collect()
      val loss = middle.map(f => f._1).sum / middle.map(_._2).sum.toDouble
      val array = new Array[Long](3)
      middle.foreach(f => f._3.zipWithIndex.foreach(t => array(t._2) += t._1))

      NEModel.logTime(s"epoch=$epoch " +
        f"loss=$loss%2.4f " +
        s"dotTime=${array(0)} " +
        s"gradientTime=${array(1)} " +
        s"adjustTime=${array(2)} " +
        s"total=${middle.map(_._2).sum.toDouble} " +
        s"lossSum=${middle.map(_._1).sum}")
    }
  }

  def getMatrixContext(matName: String): MatrixContext = {
    require(numNodesPerRow < maxNodesPerRow, s"size exceed, $numNodesPerRow * $maxNodesPerRow")

    val rowCapacity = if (numNodesPerRow > 0) numNodesPerRow else Int.MaxValue / sizeOccupiedPerNode
    val numRow = (numNode - 1) / rowCapacity + 1
    val numNodesEachRow = if (numNodesPerRow > 0) numNodesPerRow else (numNode - 1) / numRow + 1
    val numCol = numPart.toLong * numNodesEachRow * sizeOccupiedPerNode
    val rowsInBlock = numRow
    val colsInBlock = numNodesEachRow * sizeOccupiedPerNode
    val validIndexNum = -1

    NEModel.logTime(s"matrix meta:\n" +
      s"colNum: $numCol\n" +
      s"rowNum: $numRow\n" +
      s"colsInBlock: $colsInBlock\n" +
      s"rowsInBlock: $rowsInBlock\n" +
      s"numNodesPerRow: $rowCapacity\n"
    )

    new MatrixContext(matName, numRow, numCol, validIndexNum, rowsInBlock, colsInBlock, RowType.T_FLOAT_DENSE)
  }

  override def getMatrixContext: MatrixContext = getMatrixContext(embeddingMatName)

  override def getDotFunc(data: NEDataSet, batchSeed: Int, ns: Int, partitionId: Int): GetFunc = {
    val param = new DotParam(matrixId, seed, partitionId, modelId, data.asInstanceOf[W2VDataSet].sentences)
    new Dot(param)
  }

  override def getAdjustFunc(data: NEDataSet, batchSeed: Int, ns: Int, grad: Array[Float], partitionId: Int): UpdateFunc = {
    val param = new AdjustParam(matrixId, seed, partitionId, modelId, grad, data.asInstanceOf[W2VDataSet].sentences)
    new Adjust(param)
  }
}

object Word2vecPSModel {

  def buildDataBatches(trainSet: RDD[Array[Int]], batchSize: Int): Iterator[RDD[NEDataSet]] = {
    new Iterator[RDD[NEDataSet]] with Serializable {
      override def hasNext(): Boolean = true

      override def next(): RDD[NEDataSet] = {
        trainSet.mapPartitions { iter =>
          val shuffledIter = Random.shuffle(iter)
          asWord2VecBatch(shuffledIter, batchSize)
        }
      }
    }
  }

  def asWord2VecBatch(iter: Iterator[Array[Int]], batchSize: Int): Iterator[NEDataSet] = {
    val sentences = new Array[Array[Int]](batchSize)
    new Iterator[NEDataSet] {
      override def hasNext: Boolean = iter.hasNext

      override def next(): NEDataSet = {
        var pos = 0
        while (iter.hasNext && pos < batchSize) {
          sentences(pos) = iter.next()
          pos += 1
        }
        if (pos < batchSize) W2VDataSet(sentences.take(pos)) else W2VDataSet(sentences)
      }
    }
  }

  case class W2VDataSet(sentences: Array[Array[Int]]) extends NEDataSet

}
