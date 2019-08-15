package org.apache.spark.angel.ml.tree.gbdt

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.angel.ml.tree.gbdt.tree.{GBDTParam, GBTNode}
import org.apache.spark.angel.ml.tree.regression.RegTree
import org.apache.spark.ml.linalg.Vector

import scala.collection.mutable.ArrayBuffer

object GBDTModel {
  type GBTTree = RegTree[GBTNode]

  def save(model: GBDTModel, path: String): Unit = {
    val oos = new ObjectOutputStream(new FileOutputStream(path))
    oos.writeObject(model)
    oos.close()
  }

  def load(path: String): GBDTModel = {
    val ois = new ObjectInputStream(new FileInputStream(path))
    ois.readObject().asInstanceOf[GBDTModel]
  }
}

import GBDTModel._
class GBDTModel(val param: GBDTParam) extends Serializable {
  private var forest: ArrayBuffer[GBTTree] = ArrayBuffer[GBTTree]()
  private var weights: ArrayBuffer[Float] = ArrayBuffer[Float]()

  def predict(instance: Vector): Array[Float] = {
    if (param.isRegression || param.numClass == 2) {
      var pred = 0.0f
      for (i <- forest.indices)
        pred += weights(i) * forest(i).predictBinary(instance)
      Array(pred)
    } else if (param.multiTree) {
      val preds = Array.ofDim[Float](param.numClass)
      for (i <- forest.indices)
        preds(i % param.numClass) += weights(i) *
          forest(i).predictBinary(instance)
      preds
    } else {
      val preds = Array.ofDim[Float](param.numClass)
      for (i <- forest.indices) {
        val p = forest(i).predictMulti(instance)
        val w = weights(i)
        for (k <- 0 until param.numClass)
          preds(k) += w * p(k)
      }
      preds
    }
  }

  def predict(instances: Array[Vector]): Array[Array[Float]] = {
    instances.map(predict)
  }

  def get(treeId: Int): GBTTree = forest(treeId)

  def add(tree: GBTTree, weight: Float): Unit = {
    forest += tree
    weights += weight
  }

  def keepFirstTrees(num: Int): Unit = {
    forest = forest.slice(0, num)
    weights = weights.slice(0, num)
  }

  def numTree: Int = forest.size
}
