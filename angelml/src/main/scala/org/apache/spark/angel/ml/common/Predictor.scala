/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.spark.angel.ml.common

import com.tencent.angel.mlcore.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.math2.utils.{DataBlock, LabeledData}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.angel.ml.common.MathImplicits._
import com.tencent.angel.sona.core.ExecutorContext
import com.tencent.angel.sona.data.LocalMemoryDataBlock
import com.tencent.angel.sona.ml.AngelGraphModel
import org.apache.spark.angel.ml.linalg
import org.apache.spark.angel.ml.linalg.Vectors
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
        storage.put(new LabeledData(row.get(featIdx).asInstanceOf[linalg.Vector], 0.0))
        cachedRows(count % batchSize) = row
        count += 1
      case row =>
        storage.put(new LabeledData(row.get(featIdx).asInstanceOf[linalg.Vector], 0.0))
        cachedRows(count % batchSize) = row
        count += 1
    }

    predictInternal(localModel, storage, cachedRows, result)

    executorContext.returnModel(localModel)

    result.toIterator
  }

  private def predictInternal(model: AngelGraphModel,
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

  def predictRaw(features: linalg.Vector): linalg.Vector = {
    val localModel = executorContext.borrowModel(bcConf.value)

    val res = localModel.predict(new LabeledData(features, 0.0))

    executorContext.returnModel(localModel)
    Vectors.dense(res.pred, -res.pred)
  }
}
