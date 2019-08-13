package com.tencent.angel.sona.ml

import com.tencent.angel.mlcore.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.mlcore.network.{Graph, PlaceHolder}
import com.tencent.angel.mlcore.utils.JsonUtils
import com.tencent.angel.mlcore.variable.{VariableManager, VariableProvider}
import com.tencent.angel.ml.core.variable.CILSImpl
import com.tencent.angel.mlcore.{GraphModel, PredictResult}
import com.tencent.angel.ml.core.PSVariableProvider
import com.tencent.angel.ml.math2.utils.{DataBlock, LabeledData}
import com.tencent.angel.sona.data.LocalMemoryDataBlock


class AngeGraphModel(conf: SharedConf, val numTask: Int) extends GraphModel(conf) {
  private implicit val sharedConf: SharedConf = conf
  override val isSparseFormat: Boolean = conf.get(MLCoreConf.ML_IS_DATA_SPARSE, "false").toBoolean

  // protected val placeHolder: PlaceHolder = new PlaceHolder(conf)
  override protected implicit val variableManager: VariableManager = SparkPSVariableManager.get(isSparseFormat, conf)
  private implicit val cilsImpl: CILSImpl = new SparkCILSImpl(conf)
  override protected val variableProvider: VariableProvider = new PSVariableProvider(dataFormat, sharedConf)

  override implicit val graph: Graph = new Graph(variableProvider, conf, numTask)

  override def buildNetwork(): this.type = {
    JsonUtils.layerFromJson(graph)

    this
  }

  override def predict(storage: DataBlock[LabeledData]): List[PredictResult] = {
    // new MemoryDataBlock[PredictResult](storage.size())

    val numSamples = storage.size()
    val batchData = new Array[LabeledData](numSamples)
    (0 until numSamples).foreach { idx => batchData(idx) = storage.loopingRead() }
    graph.feedData(batchData)

    if (isSparseFormat) {
      pullParams(-1, graph.placeHolder.getIndices)
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
