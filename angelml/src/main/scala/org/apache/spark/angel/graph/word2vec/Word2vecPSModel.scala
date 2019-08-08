package org.apache.spark.angel.graph.word2vec


import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.sona.context.PSContext
import org.apache.spark.angel.psf.embedding.CBowModel
import org.apache.spark.angel.psf.embedding.bad._
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.spark.angel.graph.utils.NEModel
import org.apache.spark.angel.graph.utils.NEModel.NEDataSet
import org.apache.spark.angel.graph.word2vec.Word2vecPSModel.W2VDataSet
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
                      numNodesPerRow: Int = -1,
                      seed: Int)
  extends NEModel(numNode, dimension, numPart, numNodesPerRow, 2, false, seed) {

  private val maxNodesPerRow: Int = 100000

  private def sgdForBatch(partitionId: Int,
                          batchId: Int,
                          negative: Int,
                          window: Int,
                          alpha: Float,
                          batch: NEDataSet): (Double, Long, Array[Long]) = {

    var (start, end) = (0L, 0L)
    val seed = 2017

    // calculate index
    start = System.currentTimeMillis()
    val model = new CBowModel(window, negative, alpha, numNode, dimension)
    val sentences = batch.asInstanceOf[W2VDataSet].sentences
    val indices = model.indicesForCbow(sentences, seed)
    end = System.currentTimeMillis()
    val calcuIndexTime = end - start

    // pull
    start = System.currentTimeMillis()
    val pullParams = new W2VPullParam(psMatrix.id, indices, numNodesPerRow, dimension)
    val result = psMatrix.psfGet(new W2VPull(pullParams)).asInstanceOf[W2VPullResult]
    end = System.currentTimeMillis()
    val pullTime = end - start

    val index = new Int2IntOpenHashMap()
    index.defaultReturnValue(-1)
    for (i <- 0 until indices.length) index.put(indices(i), i)
    val deltas = new Array[Float](result.layers.length)

    // cbow
    start = System.currentTimeMillis()
    val loss = model.cbow(sentences, seed, result.layers, index, deltas)
    end = System.currentTimeMillis()
    val cbowTime = end - start

    // push
    start = System.currentTimeMillis()
    val pushParams = new W2VPushParam(psMatrix.id, indices, deltas, numNodesPerRow, dimension)
    psfUpdate(new W2VPush(pushParams))
    end = System.currentTimeMillis()
    val pushTime = end - start

    //    val batchSize = batch.asInstanceOf[W2VDataSet].sentences.map(_.length).sum
    //    println(s"${loss._1/loss._2} learnRate=$alpha length=${loss._2} batchSize=$batchSize")
    (loss._1, loss._2.toLong, Array(calcuIndexTime, pullTime, cbowTime, pushTime))
  }

  private def sgdForPartition(partitionId: Int,
                              iterator: Iterator[NEDataSet],
                              window: Int,
                              negative: Int,
                              alpha: Float): Iterator[(Double, Long, Array[Long])] = {
    PSContext.instance()
    val r = iterator.zipWithIndex.map(batch => sgdForBatch(partitionId, batch._2,
      window, negative, alpha, batch._1))
      .reduce { (f1, f2) =>
        (f1._1 + f2._1,
          f1._2 + f2._2,
          f1._3.zip(f2._3).map(f => f._1 + f._2))
      }
    Iterator.single(r)
  }

  def train(corpus: RDD[Array[Int]]): Unit = {
    psfUpdate(getInitFunc(corpus.getNumPartitions, numNode, maxLength, negSample, windowSize))
    val trainBatches = Word2vecPSModel.buildDataBatches(corpus, batchSize)

    for (epoch <- 1 to numEpoch) {
      val data = trainBatches.next()
      val middle = data.mapPartitionsWithIndex((partitionId, iterator) =>
        sgdForPartition(partitionId, iterator, windowSize, negSample, learningRate.toFloat),
        true).reduce { case (f1, f2) =>
        (f1._1 + f2._1, f1._2 + f2._2, f1._3.zip(f2._3).map(f => f._1 + f._2))
      }
      val loss = middle._1 / middle._2.toDouble
      val array = middle._3

      NEModel.logTime(s"epoch=$epoch " +
        f"loss=$loss%2.4f " +
        s"calcuIndexTime=${array(0)} " +
        s"pullTime=${array(1)} " +
        s"cbowTime=${array(2)} " +
        s"pushTime=${array(3)} " +
        s"total=${middle._2} " +
        s"lossSum=${middle._1}")
    }
  }

  def getMatrixContext(matName: String): MatrixContext = {
    require(numNodesPerRow < maxNodesPerRow, s"size exceed, $numNodesPerRow * $maxNodesPerRow")

    val rowCapacity = if (numNodesPerRow > 0) numNodesPerRow else Int.MaxValue / dimension
    val numCol = rowCapacity * dimension * 2
    val numRow = numNode / rowCapacity + 1
    val rowsInBlock = numRow / numPart + 1
    val colsInBlock = numCol
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

  override def randomInitialize(seed: Int): Unit = {
    psfUpdate(new W2VRandom(new W2VRandomParam(psMatrix.id, dimension)))
  }

  override def getMatrixContext: MatrixContext = ???
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
