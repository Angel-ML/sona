package org.apache.spark.angel.graph.word2vec

import com.tencent.angel.exception.AngelException
import org.apache.spark.angel.ml.Estimator
import org.apache.spark.angel.ml.param.ParamMap
import org.apache.spark.angel.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class Word2Vec(override val uid: String) extends Estimator[Word2VecModel] with Word2VecParams
  with DefaultParamsWritable with Logging {

  def this() = this(Identifiable.randomUID("Word2Vec"))

  lazy val modelId: Int = getModel match {
    case "skipgram" => 0
    case "cbow" => 1
    case _ => throw new AngelException("model type should be cbow or skipgram")
  }

  override def fit(dataset: Dataset[_]): Word2VecModel = {
    val data = dataset.select($(input)).rdd.map {
      case Row(arr: Array[Int]) => arr
      case row: GenericRowWithSchema =>
        row.get(0).asInstanceOf[mutable.WrappedArray[Int]].toArray
    }

    val word2Vec = new Word2vecPSModel(getEmbeddingMatrixName, getMaxIndex, getEmbeddingDim, getModel, getNumPSPart, getNumEpoch,
      getWindowSize, getNegSample, getMaxLength, getStepSize, getBatchSize, getNodesNumPerRow, getSeed.toInt)

    word2Vec.train(data)

    new Word2VecModel(uid, word2Vec.psMatrix)
  }

  override def copy(extra: ParamMap): Word2Vec = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    // this method not transformSchema, just check schema
    validateAndTransformSchema(schema)
  }
}


object Word2Vec extends DefaultParamsReadable[Word2Vec] with Logging {
  override def load(path: String): Word2Vec = super.load(path)
}

