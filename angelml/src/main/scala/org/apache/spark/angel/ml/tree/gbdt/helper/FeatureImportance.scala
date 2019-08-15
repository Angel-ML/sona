package org.apache.spark.angel.ml.tree.gbdt.helper

import org.apache.spark.angel.ml.tree.basic.split.{SplitEntry, SplitType}
import org.apache.spark.angel.ml.tree.gbdt.GBDTModel

import scala.collection.JavaConversions._
import scala.collection.mutable

object FeatureImportance {
  val WEIGHT: String = "weight"
  val GAIN: String = "gain"
  val TOTAL_GAIN: String = "total_gain"
  val DEFAULT_ML_GBDT_IMPORTANCE_TYPE: String = GAIN

  def ensureImportanceType(importanceType: String): Unit = {
    val provided = Array(WEIGHT, GAIN, TOTAL_GAIN)
    require(provided.contains(importanceType),
      s"Unsupported feature importance type: $importanceType, " +
        s"choices: [${provided.mkString(", ")}]")
  }

  /**
    * Calculate the feature importance based on a GBDT model
    * using predefined importance type.
    *
    * @param model GBDT model
    * @param importanceType Predefined importance type, three types supported
    *                       - weight: the number of occurrences
    *                       - gain: the average split gain (average over occurrences)
    *                       - total_gain: the total split gain
    * @return Seq of (feature index, importance) pairs, sorted by importance.
    */
  def featImportance(model: GBDTModel, importanceType: String): Seq[(Int, Float)] = {
    importanceType match {
      case WEIGHT =>
        featImportance(model, (_: Int, _: Int, _: SplitEntry) => 1f, average = false)
      case GAIN =>
        featImportance(model, (_: Int, _: Int, s: SplitEntry) => s.getGain, average = true)
      case TOTAL_GAIN =>
        featImportance(model, (_: Int, _: Int, s: SplitEntry) => s.getGain, average = false)
      case _ => throw new RuntimeException("No such predefined " +
        "feature importance type: " + importanceType)
    }
  }

  /**
    * Calculate the feature importance based on a GBDT model
    * with specific importance function.
    *
    * @param model GBDT model
    * @param importanceFunc Function to calculate importance of one tree node
    * @param average If true, the importance will be divided by number of occurrences
    * @return Seq of (feature index, importance) pairs, sorted by importance.
    */
  def featImportance(model: GBDTModel, importanceFunc: (Int, Int, SplitEntry) => Float,
                     average: Boolean): Seq[(Int, Float)] = {
    val featStats = mutable.Map[Int, (Int, Float)]()

    for (treeId <- 0 until model.numTree) {
      val tree = model.get(treeId)
      tree.getNodes.foreach {
        case (nodeId, node) =>
          if (!node.isLeaf) {
            val split = node.getSplitEntry
            val fid = split.getFid
            val importance = importanceFunc(treeId, nodeId, split)
            if (featStats.contains(fid)) {
              val (cnt, sum) = featStats(fid)
              featStats(fid) = (cnt + 1, sum + importance)
            } else {
              featStats(fid) = (1, importance)
            }
          }
      }
    }

    if (average) {
      featStats.mapValues(pair => pair._2 / pair._1).toSeq.sortBy(-_._2)
    } else {
      featStats.mapValues(pair => pair._2).toSeq.sortBy(-_._2)
    }
  }

  private def examples(): Unit = {
    import org.apache.spark.SparkContext
    val sc = SparkContext.getOrCreate()

    val modelPath = "hdfs://path/to/model"
    val model = sc.objectFile[GBDTModel](modelPath).collect().head

    // Examples of predefined importance function:
    // Calculate by number of occurrences
    featImportance(model, importanceType = "weight")
    // Calculate by average split gain
    featImportance(model, importanceType = "gain")
    // Calculate by total split gain
    featImportance(model, importanceType = "total_gain")

    // Examples of self-customized importance function:
    // Calculate by total split gain, decayed by tree index
    featImportance(model, (treeId: Int, _: Int, s: SplitEntry) =>
      s.getGain * math.pow(0.9, treeId).toFloat, average = false)
    // Calculate by average split gain, decayed by node depth
    featImportance(model, (_: Int, nodeId: Int, s: SplitEntry) =>
      s.getGain * math.pow(0.9, math.floor(math.log10(nodeId + 1) / math.log10(2))).toFloat,
      average = true)
    // Only consider continuous features
    featImportance(model, (_: Int, _: Int, s: SplitEntry) =>
      if (s.splitType() == SplitType.SPLIT_POINT) s.getGain else 0f, average = true)
  }
}
