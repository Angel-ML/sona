package org.apache.spark.angel.graph.params

import org.apache.spark.angel.ml.param.{DoubleParam, IntParam, LongParam, Param, Params}

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

  setDefault(nodesNumPerRow, -1)

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

