package com.tencent.angel.sona.ml

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.core.local.data.LocalMemoryDataBlock
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers.PlaceHolder
import com.tencent.angel.ml.core.utils.JsonUtils
import com.tencent.angel.ml.core.variable.{CILSImpl, VariableManager, VariableProvider}
import com.tencent.angel.ml.core.{GraphModel, PSVariableProvider, PredictResult}
import com.tencent.angel.ml.math2.utils.LabeledData


class AngeGraphModel(sharedConf: SharedConf, val numTask: Int) extends GraphModel {
  override val isSparseFormat: Boolean = sharedConf.getBoolean(MLCoreConf.ML_IS_DATA_SPARSE)

  override protected val placeHolder: PlaceHolder = new PlaceHolder(sharedConf)
  override protected implicit val variableManager: VariableManager = SparkPSVariableManager.get(isSparseFormat)
  private implicit val cilsImpl: CILSImpl = new SparkCILSImpl()
  override protected val variableProvider: VariableProvider = new PSVariableProvider(dataFormat, modelType, placeHolder)

  override implicit val graph: Graph = new Graph(variableProvider, placeHolder, sharedConf, numTask)

  override def buildNetwork(): this.type = {
    JsonUtils.layerFromJson(sharedConf.getJson)

    this
  }

  override def predict(storage: DataBlock[LabeledData]): List[PredictResult] = {
    // new MemoryDataBlock[PredictResult](storage.size())

    val numSamples = storage.size()
    val batchData = new Array[LabeledData](numSamples)
    (0 until numSamples).foreach { idx => batchData(idx) = storage.loopingRead() }
    graph.feedData(batchData)

    if (isSparseFormat) {
      pullParams(-1, placeHolder.getIndices)
    } else {
      pullParams(-1)
    }

    graph.predict()
  }

  override def predict(data: LabeledData): PredictResult = {
    val storage = new LocalMemoryDataBlock(1, 1024)
    storage.put(data)
    predict(storage).head
  }
}
