package org.apache.spark.angelml.common

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.core.local.data.LocalMemoryDataBlock
import com.tencent.angel.ml.math2.utils.LabeledData
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.angelml.linalg.{Vector, Vectors}
import org.apache.spark.angelml.common.MathImplicits._
import com.tencent.angel.sona.core.ExecutorContext
import com.tencent.angel.sona.ml.AngeGraphModel
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SPKSQLUtils}

import scala.collection.mutable.ListBuffer

class Predictor(bcValue: Broadcast[ExecutorContext],
                featIdx: Int, predictionCol: String, probabilityCol: String,
                bcConf: Broadcast[SharedConf]) extends Serializable {

  @transient private lazy val executorContext: ExecutorContext = {
    bcValue.value
  }

  @transient private lazy implicit val dim: Long = {
    executorContext.conf.getLong(MLCoreConf.ML_FEATURE_INDEX_RANGE)
  }

  @transient private lazy val appendedSchema: StructType = if (probabilityCol.nonEmpty) {
    new StructType(Array[StructField](StructField(probabilityCol, DoubleType),
      StructField(predictionCol, DoubleType)))
  } else {
    new StructType(Array[StructField](StructField(predictionCol, DoubleType)))
  }

  def predictRDD(data: Iterator[Row]): Iterator[Row] = {
    val localModel = executorContext.borrowModel(bcConf.value)
    val batchSize = 1024
    val storage = new LocalMemoryDataBlock(batchSize, batchSize * 1024 * 1024)

    var count = 0
    val cachedRows: Array[Row] = new Array[Row](batchSize)
    val result: ListBuffer[Row] = ListBuffer[Row]()
    data.foreach {
      case row if count != 0 && count % batchSize == 0 =>
        predictInternal(localModel, storage, cachedRows, result)

        storage.clean()
        storage.put(new LabeledData(row.get(featIdx).asInstanceOf[Vector], 0.0))
        cachedRows(count % batchSize) = row
        count += 1
      case row =>
        storage.put(new LabeledData(row.get(featIdx).asInstanceOf[Vector], 0.0))
        cachedRows(count % batchSize) = row
        count += 1
    }

    predictInternal(localModel, storage, cachedRows, result)

    executorContext.returnModel(localModel)

    result.toIterator
  }

  private def predictInternal(model: AngeGraphModel,
                              storage: DataBlock[LabeledData],
                              cachedRows: Array[Row],
                              result: ListBuffer[Row]): Unit = {
    val predicted = model.predict(storage)

    if (appendedSchema.length == 1) {
      predicted.zipWithIndex.foreach {
        case (res, idx) =>
          result.append(SPKSQLUtils.append(cachedRows(idx), appendedSchema, res.pred))
      }
    } else {
      predicted.zipWithIndex.foreach {
        case (res, idx) =>
          result.append(SPKSQLUtils.append(cachedRows(idx), appendedSchema, res.proba, res.predLabel))
      }
    }

  }

  def predictRaw(features: Vector): Vector = {
    val localModel = executorContext.borrowModel(bcConf.value)

    val res = localModel.predict(new LabeledData(features, 0.0))

    executorContext.returnModel(localModel)
    Vectors.dense(res.pred, -res.pred)
  }
}
