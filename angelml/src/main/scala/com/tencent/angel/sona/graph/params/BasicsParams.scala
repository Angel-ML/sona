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

package com.tencent.angel.sona.graph.params
import com.tencent.angel.sona.ml.param.{DoubleParam, IntParam, LongParam, Param, Params}

trait HasWindowSize extends Params {

  final val windowSize = new IntParam(this, "windowSize", "windowSize")

  final def getWindowSize: Int = $(windowSize)

  setDefault(windowSize, -1)

  final def setWindowSize(num: Int): this.type = set(windowSize, num)

}

trait HasEmbeddingDim extends Params {

  final val embeddingDim = new IntParam(this, "embeddingDim", "embeddingDim")

  final def getEmbeddingDim: Int = $(embeddingDim)

  setDefault(embeddingDim, -1)

  final def setEmbeddingDim(num: Int): this.type = set(embeddingDim, num)

}

trait HasNegSample extends Params {

  final val negSample = new IntParam(this, "negSample", "negSample")

  final def getNegSample: Int = $(negSample)

  setDefault(negSample, -1)

  final def setNegSample(num: Int): this.type = set(negSample, num)

}

trait HasMaxIndex extends Params {

  final val maxIndex = new IntParam(this, "maxIndex", "maxIndex")

  final def getMaxIndex: Int = $(maxIndex)

  setDefault(maxIndex, -1)

  final def setMaxIndex(num: Int): this.type = set(maxIndex, num)

}

trait HasNumPSPart extends Params {

  final val numPSPart = new IntParam(this, "numPSPart", "numPSPart")

  final def getNumPSPart: Int = $(numPSPart)

  setDefault(numPSPart, -1)

  final def setNumPSPart(num: Int): this.type = set(numPSPart, num)

}

trait HasNodesNumPerRow extends Params {

  final val nodesNumPerRow = new IntParam(this, "nodesNumPerRow", "nodesNumPerRow")

  final def getNodesNumPerRow: Int = $(nodesNumPerRow)

  setDefault(nodesNumPerRow, 1)

  final def setNodesNumPerRow(num: Int): this.type = set(nodesNumPerRow, num)

}

trait HasOrder extends Params {

  final val order = new IntParam(this, "order", "order")

  final def getOrder: Int = $(order)

  setDefault(order, -1)

  final def setOrder(num: Int): this.type = set(order, num)

}

trait HasMaxLength extends Params {

  final val maxLength = new IntParam(this, "maxLength", "maxLength")

  final def getMaxLength: Int = $(maxLength)

  setDefault(maxLength, -1)

  final def setMaxLength(num: Int): this.type = set(maxLength, num)

}

trait HasModel extends Params {

  final val model = new Param[String](this, "model", "model")

  final def getModel: String = $(model)

  setDefault(model, "")

  final def setModel(m: String): this.type = set(model, m)

}

trait HasEmbeddingMatrixName extends Params {

  final val embeddingMatrixName = new Param[String](this, "embeddingMatrixName", "embeddingMatrixName")

  final def getEmbeddingMatrixName: String = $(embeddingMatrixName)

  setDefault(embeddingMatrixName, "embedding")

  final def setEmbeddingMatrixName(m: String): this.type = set(embeddingMatrixName, m)
}

trait HasVersion extends Params {

  final val version = new Param[String](this, "version", "version")

  final def getVersion: String = $(version)

  setDefault(version, "v2")

  final def setVersion(m: String): this.type = set(version, m)

}

trait HasModelPath extends Params {

  final val modelPath = new Param[String](this, "modelPath", "modelPath")

  final def getModelPath: String = $(modelPath)

  setDefault(modelPath, "")

  final def setModelPath(mp: String): this.type = set(modelPath, mp)

}

trait HasNumRowDataSet extends Params {

  final val numRowDataSet = new LongParam(this, "numRowDataSet", "numRowDataSet")

  final def getNumRowDataSet: Long = $(numRowDataSet)

  setDefault(numRowDataSet, -1L)

  final def setNumRowDataSet(num: Long): this.type = set(numRowDataSet, num)

}

trait HasSampleRate extends Params {

  final val sampleRate = new DoubleParam(this, "sampleRate", "sampleRate")

  final def getSampleRate: Double = $(sampleRate)

  setDefault(sampleRate, 1.0)

  final def setSampleRate(num: Double): this.type = set(sampleRate, num)

}

trait HasModelSaveInterval extends Params {
  final val modelSaveInterval = new IntParam(this, "modelSaveInterval", "modelSaveInterval")

  final def getModelSaveInterval: Int = $(modelSaveInterval)

  setDefault(modelSaveInterval, 10)

  final def setModelSaveInterval(num: Int): this.type = set(modelSaveInterval, num)
}

trait HasOutput extends Params {
  final val output = new Param[String](this, "output", "output")

  final def getOutput: String = $(output)

  setDefault(output, "")

  final def setOutput(out: String): this.type = set(output, out)
}


